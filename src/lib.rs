// DuckDB loadable extension: BM25 full-text search, tokenization, and Snowball
// stemming, powered by tantivy.
//
// This file is a thin quack-rs binding layer over `search.rs` (a pure Rust
// search core with no DuckDB dependency). The search core builds an ephemeral
// RAM index per call; this layer plumbs SQL inputs into it and returns
// vectors/hits back to DuckDB.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use libduckdb_sys::{duckdb_data_chunk_get_size, duckdb_vector_size};
use quack_rs::connection::{Connection, Registrar};
use quack_rs::data_chunk::DataChunk;
use quack_rs::error::ExtensionError;
use quack_rs::query::OwnedConnection;
use quack_rs::scalar::{
    ScalarFunctionBuilder, ScalarFunctionInfo, ScalarFunctionSetBuilder, ScalarOverloadBuilder,
};
use quack_rs::table::{BindInfo, FfiBindData, FfiInitData, InitInfo, TableFunctionBuilder};
use quack_rs::types::{LogicalType, TypeId};
use quack_rs::vector::{ListBuilder, VectorReader, VectorWriter};

mod search;

// ─────────────────────────────────────────────────────────────────────────────
// Session connection — v0.1 single-database semantics, made loud & safe
//
// Table-function callbacks (bind/init/scan) have no `duckdb_connection` — the
// C API only hands them a client context, and C-API v1.2.0 provides no accessor
// from a client context back to its `duckdb_database`. The only handle to a
// database this extension sees is the entry point's `Connection`, so
// `bm25_search` runs SQL (`SELECT id, text FROM {table}`) through ONE dedicated
// `OwnedConnection` opened from the first `LOAD`'s database.
//
// Consequences, and how each is made safe (this was the P0 review finding):
//
//   * **One database per process.** The dedicated connection is bound to the
//     FIRST database that loaded the extension. If a *second, different*
//     database loads it, `register_all` flips `MULTI_DB` and every
//     `bm25_search` thereafter fails with a clear error — so a query issued
//     through the second database can no longer silently read the first
//     database's tables. (True per-database dispatch is impossible in C-API
//     v1.2.0: a callback cannot recover its own `duckdb_database`. The proper
//     fix is to run corpus acquisition through the caller's query plan, which
//     removes the dedicated connection entirely — tracked as a v0.2 task.)
//
//   * **Transaction visibility.** The dedicated connection has its own
//     transaction snapshot, so `bm25_search` does NOT see the caller's
//     uncommitted writes or temporary tables. Documented in the README; a real
//     fix also requires the caller-plan redesign above.
//
//   * **Re-entry.** A `bm25_search` reached *recursively* (e.g. through a view
//     whose definition calls `bm25_search`, while the outer search is already
//     querying the dedicated connection) would otherwise deadlock on the
//     session mutex or recurse to a stack overflow. A thread-local guard is set
//     while a query runs on the dedicated connection; a nested `bm25_search`
//     on the same thread sees it and returns a clear error instead.
// ─────────────────────────────────────────────────────────────────────────────

/// The one dedicated connection, opened from the first `LOAD`'s database.
static SESSION: OnceLock<Mutex<OwnedConnection>> = OnceLock::new();
/// The `duckdb_database` that `SESSION` belongs to (the first loader). Used to
/// detect when a *second, different* database loads the extension. An
/// `AtomicPtr` rather than a `OnceLock` around a raw pointer: raw pointers are
/// neither `Send` nor `Sync`, so a `OnceLock` holding one cannot back a
/// `static`. The null slot is claimed with a compare-exchange, which preserves
/// the old race semantics (first caller wins; losers keep the winner's identity).
static FIRST_DB: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
/// Set once a second, different database loads the extension. When set, every
/// `bm25_search` fails loudly instead of reading the first database's tables.
static MULTI_DB: AtomicBool = AtomicBool::new(false);

/// Non-zero while the *current thread* is executing a query on the dedicated
/// connection. This is what makes re-entry detectable: a nested `bm25_search`
/// on the same thread (a view that calls `bm25_search`) reads this and bails
/// out with a clear error instead of deadlocking on the session mutex or
/// recursing to a stack overflow.
thread_local! {
    static IN_QUERY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Why the dedicated connection could not be reached. Each maps to a distinct,
/// actionable SQL error message (see [`SessionError::message`]).
enum SessionError {
    /// A `bm25_search` is already running on this thread (re-entry via a view
    /// or nested table function).
    Reentrant,
    /// The extension was loaded on more than one database in this process.
    MultiDb,
    /// The session connection was never initialized (should not happen after a
    /// successful `LOAD`).
    Unavailable,
}

impl SessionError {
    fn message(&self) -> &'static str {
        match self {
            Self::Reentrant => {
                "bm25_search: nested bm25_search detected (a view or table function is \
                 calling bm25_search while another bm25_search is executing on this \
                 thread); this is not supported"
            }
            Self::MultiDb => {
                "bm25_search: the tantivy extension was loaded on more than one database \
                 in this process; v0.1 supports a single database instance, so searches \
                 could not be attributed to a database (they would read the first \
                 database's tables)"
            }
            Self::Unavailable => "bm25_search: the extension session is unavailable",
        }
    }
}

/// Set/clear the thread-local re-entry mark. Owned by [`SessionConn`] so the
/// mark is set for exactly as long as the session lock is held and cleared on
/// drop (even across an early return or panic in the query closure).
struct ReentryMark;
impl ReentryMark {
    fn new() -> Self {
        IN_QUERY.with(|c| c.set(true));
        ReentryMark
    }
}
impl Drop for ReentryMark {
    fn drop(&mut self) {
        IN_QUERY.with(|c| c.set(false));
    }
}

/// Holds the session lock plus the re-entry mark for the current thread, both
/// for the struct's whole lifetime. A query reached from within this query
/// sees the mark (via [`session`]) and fails with [`SessionError::Reentrant`]
/// rather than deadlocking on the mutex.
struct SessionConn {
    con: MutexGuard<'static, OwnedConnection>,
    _mark: ReentryMark,
}

/// Reach the dedicated connection for a query, or a clear [`SessionError`].
/// The returned guard holds the session lock and sets the thread-local
/// re-entry mark until it is dropped.
fn session() -> Result<SessionConn, SessionError> {
    if IN_QUERY.with(|c| c.get()) {
        return Err(SessionError::Reentrant);
    }
    if MULTI_DB.load(Ordering::SeqCst) {
        return Err(SessionError::MultiDb);
    }
    let m = SESSION.get().ok_or(SessionError::Unavailable)?;
    // `.lock()` (not `try_lock`): cross-thread concurrency on the single
    // shared connection is serialized, preserving the pre-existing behaviour
    // (a second concurrent search waits for the first to release the lock).
    // Same-thread re-entry is already rejected above via the thread-local mark,
    // so we never block on a lock this thread already holds.
    let g = match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    Ok(SessionConn { con: g, _mark: ReentryMark::new() })
}

#[cfg(test)]
fn reentry_marked() -> bool {
    IN_QUERY.with(|c| c.get())
}

// ─────────────────────────────────────────────────────────────────────────────
// Process-global registry of persistent indexes (v0.3)
//
// `tantivy_create_index` / `tantivy_load_index` register a
// `search::PersistentIndex` here by name; `tantivy_search_index` /
// `tantivy_list` / `tantivy_save_index` read it. The registry is a
// process-global `OnceLock<Mutex<..>>` (the duckdb-vss pattern): it outlives
// any single database, and every access is a short-lived lock.
//
// Lock discipline (deadlock-free by construction):
//   * the registry lock is held only long enough to clone/insert/collect
//     `Arc`s — never across a search, a build, or a file copy;
//   * the global lock order is session → build → registry. The search/list/
//     save/load paths take ONLY the registry lock (never the session lock),
//     so no lock cycle can form;
//   * `CREATE_INDEX_LOCK` serializes index builds (wipe + write + register)
//     so two concurrent builds of the same name cannot interleave in the
//     index directory.
// ─────────────────────────────────────────────────────────────────────────────

/// The one process-global registry of persistent indexes, keyed by name.
static REGISTRY:
    OnceLock<Mutex<HashMap<String, Arc<search::PersistentIndex>>>> = OnceLock::new();

/// Serializes index builds (see the section note above).
static CREATE_INDEX_LOCK: Mutex<()> = Mutex::new(());

/// Borrow the registry (initializing it on first use).
fn registry() -> &'static Mutex<HashMap<String, Arc<search::PersistentIndex>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Lock the registry, recovering from a poisoned lock: one panicking callback
/// must not brick the registry for the rest of the process.
fn registry_lock() -> MutexGuard<'static, HashMap<String, Arc<search::PersistentIndex>>> {
    match registry().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// A conservative identifier check: `[A-Za-z_][A-Za-z0-9_]*`, optionally
/// `schema.table`. Table/column names are interpolated into SQL text, so we
/// only accept shapes DuckDB parses as plain identifiers — this is what keeps
/// the interpolation from being an injection vector.
fn is_safe_identifier(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() > 2 {
        return false;
    }
    for p in &parts {
        if p.is_empty() {
            return false;
        }
        let bytes = p.as_bytes();
        if !bytes[0].is_ascii_alphabetic() && bytes[0] != b'_' {
            return false;
        }
        if !bytes.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_') {
            return false;
        }
    }
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Table function: bm25_search(table, query, id_column, text_column, language, limit)
//   → doc_id BIGINT, score DOUBLE
//
// bind  : read + validate parameters; confirm the table and columns exist
//         (a `LIMIT 0` probe on the session connection, so the error message
//         names the problem before any rows are touched).
// init  : stream the selected rows into `search::Document`s, run BM25 once,
//         and keep the ranked hits.
// scan  : emit the hits in batches.
// ─────────────────────────────────────────────────────────────────────────────

struct Bm25Bind {
    table: String,
    query: String,
    id_column: String,
    text_column: String,
    language: String,
    limit: i64,
}

struct Bm25State {
    hits: Vec<search::Hit>,
    next: usize,
}

quack_rs::table_bind_callback!(bm25_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        // table and query are the two required POSITIONAL parameters;
        // id_column / text_column / language / top_k are NAMED parameters with
        // defaults, read via get_named_parameter_value (None or NULL → default).
        // This lets callers write `bm25_search('docs','cat')` (all defaults) or
        // `bm25_search('docs','cat', id_column:='id', top_k:=10)` — matching the
        // documented signature in the brief.
        let table = bind_info.get_parameter_value(0).as_str().unwrap_or_default();
        let query = bind_info.get_parameter_value(1).as_str().unwrap_or_default();

        let id_column = bind_info
            .get_named_parameter_value("id_column")
            .as_str()
            .unwrap_or_default();
        let text_column = match bind_info.get_named_parameter_value("text_column").as_str() {
            Ok(s) if !s.trim().is_empty() => s,
            _ => "body".to_string(),
        };
        let language = match bind_info.get_named_parameter_value("language").as_str() {
            Ok(s) if !s.trim().is_empty() => s,
            _ => "english".to_string(),
        };
        // `top_k` must be within [1, 100_000]: below 1 is a semantic error and
        // above the cap tantivy's TopDocs allocation (2*limit) becomes a memory
        // bomb. Out-of-range values are rejected at bind time — never silently
        // clamped, never allowed to grow the collector unboundedly.
        let limit = match bind_info.get_named_parameter_value("top_k").as_i64() {
            Some(v) if (1..=100_000).contains(&v) => v,
            Some(v) => {
                bind_info.set_error(&format!(
                    "bm25_search: top_k must be between 1 and 100000 (got {v})"
                ));
                return;
            }
            None => 100, // named arg absent (or NULL) → default
        };

        let id_column = id_column.trim().to_string();

        if !is_safe_identifier(&table) {
            bind_info.set_error(
                "bm25_search: table_name must be an identifier like 'docs' or 'main.docs'",
            );
            return;
        }
        if !id_column.is_empty() && !is_safe_identifier(&id_column) {
            bind_info.set_error(
                "bm25_search: id_column must be an identifier (e.g. 'doc_id') or empty for auto ids",
            );
            return;
        }
        if !is_safe_identifier(&text_column) {
            bind_info.set_error("bm25_search: text_column must be an identifier (e.g. 'body')");
            return;
        }

        // Probe the table + columns on the session connection. `LIMIT 0` means
        // no rows are evaluated, so this only fails if the table/columns are
        // missing or the casts are not type-legal — exactly the clear
        // bind-time error we want.
        let probe = if id_column.is_empty() {
            format!("SELECT {text_column}::VARCHAR FROM {table} LIMIT 0")
        } else {
            format!(
                "SELECT {id_column}::BIGINT, {text_column}::VARCHAR FROM {table} LIMIT 0"
            )
        };
        match session() {
            Ok(sess) => match sess.con.query(&probe) {
                Ok(_result) => {}
                Err(e) => {
                    bind_info.set_error(&format!("bm25_search: cannot open table '{table}': {e}"));
                    return;
                }
            },
            Err(e) => {
                bind_info.set_error(e.message());
                return;
            }
        }

        bind_info
            .add_result_column("doc_id", TypeId::BigInt)
            .add_result_column("score", TypeId::Double)
            .set_cardinality(limit as u64, false);
        FfiBindData::<Bm25Bind>::set(
            info,
            Bm25Bind {
                table: table.trim().to_string(),
                query,
                id_column,
                text_column,
                language,
                limit,
            },
        );
    }
});

quack_rs::table_init_callback!(bm25_init, |info| {
    unsafe {
        let init_info = InitInfo::new(info);
        init_info.set_max_threads(1);

        let Some(bind) = FfiBindData::<Bm25Bind>::get_from_init(info) else {
            FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
            return;
        };

        let has_id = !bind.id_column.is_empty();
        let sql = if has_id {
            format!(
                "SELECT {}::BIGINT AS id, {}::VARCHAR AS text FROM {}",
                bind.id_column, bind.text_column, bind.table
            )
        } else {
            format!("SELECT {}::VARCHAR AS text FROM {}", bind.text_column, bind.table)
        };

        let mut docs: Vec<search::Document> = Vec::new();
        let mut seq: i64 = 0;
        {
            let sess = match session() {
                Ok(s) => s,
                Err(e) => {
                    init_info.set_error(e.message());
                    FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                    return;
                }
            };
            // Memory guard moved BEFORE materialization (P1): a cheap aggregate
            // probe measures the corpus (row count + cumulative text bytes)
            // without copying a single row, so an oversized table is rejected
            // before any of the multi-gigabyte allocations the old post-hoc
            // check only caught *after* `docs` and the RAM index had already
            // been built. `SUM(LENGTH(text))` is NULL-insensitive (a NULL text
            // contributes nothing) and `COALESCE` folds an all-NULL table to 0.
            let probe_sql = format!(
                "SELECT count(*)::BIGINT, COALESCE(sum(LENGTH({text})::BIGINT), 0)::BIGINT \
                 FROM {table}",
                text = bind.text_column,
                table = bind.table,
            );
            let mut p = match sess.con.query(&probe_sql) {
                Ok(r) => r,
                Err(e) => {
                    init_info.set_error(&format!("bm25_search: {e}"));
                    FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                    return;
                }
            };
            let chunk = match p.next_chunk() {
                Ok(Some(c)) => c,
                Ok(None) => {
                    init_info.set_error("bm25_search: pre-check returned no rows");
                    FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                    return;
                }
                Err(e) => {
                    init_info.set_error(&format!("bm25_search: {e}"));
                    FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                    return;
                }
            };
            let r = unsafe { chunk.reader(0) };
            let c = unsafe { chunk.reader(1) };
            let row_count = unsafe { r.read_i64(0) } as u64;
            let text_len = unsafe { c.read_i64(0) } as u64;
            let text_cap = (search::MAX_DOCS * 1024) as u64; // ~1KB/doc worst case
            if row_count > search::MAX_DOCS as u64 {
                init_info.set_error(&format!(
                    "bm25_search: table '{table}' has {n} rows; the limit is {d} rows — narrow the corpus with a WHERE clause or a filtered view",
                    table = bind.table,
                    n = row_count,
                    d = search::MAX_DOCS
                ));
                FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                return;
            }
            if text_len > text_cap {
                init_info.set_error(&format!(
                    "bm25_search: table '{table}' holds {t} bytes of text across {n} rows; the limit is {cap} bytes — narrow the corpus",
                    table = bind.table,
                    t = text_len,
                    n = row_count,
                    cap = text_cap
                ));
                FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                return;
            }
            let mut result = match sess.con.query(&sql) {
                Ok(r) => r,
                Err(e) => {
                    init_info.set_error(&format!("bm25_search: {e}"));
                    FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                    return;
                }
            };
            loop {
                match result.next_chunk() {
                    Ok(Some(chunk)) => {
                        let d: &DataChunk = &chunk;
                        let text_reader = unsafe { d.reader(if has_id { 1 } else { 0 }) };
                        let id_reader = has_id.then(|| unsafe { d.reader(0) });
                        for row in 0..d.size() {
                            if !unsafe { text_reader.is_valid(row) } {
                                continue; // NULL/missing text → nothing to index
                            }
                            let text = unsafe { text_reader.read_str(row) }.to_string();
                            // Per-text bound (the scalar path enforces this via
                            // search::check_text_len; kept as defense-in-depth
                            // in case one row grows between the pre-check and
                            // here, and so a single 20MB row can never be
                            // silently indexed).
                            if text.len() > search::MAX_TEXT_BYTES {
                                init_info.set_error(&format!(
                                    "bm25_search: a row in '{table}' exceeds the {b}-byte per-text limit",
                                    table = bind.table,
                                    b = search::MAX_TEXT_BYTES
                                ));
                                FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                                return;
                            }
                            let id = match &id_reader {
                                Some(r) => {
                                    if !unsafe { r.is_valid(row) } {
                                        continue;
                                    }
                                    unsafe { r.read_i64(row) }
                                }
                                None => {
                                    let id = seq;
                                    seq += 1;
                                    id
                                }
                            };
                            docs.push(search::Document { id, text });
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        init_info.set_error(&format!("bm25_search: {e}"));
                        FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                        return;
                    }
                }
            }
            // `result`, `p`, and `sess` (session lock + re-entry mark) drop here,
            // before the lock-free BM25 run below.
        }

        let hits = match search::bm25_search(&docs, &bind.query, &bind.language, bind.limit as usize)
        {
            Ok(h) => h,
            Err(e) => {
                init_info.set_error(&format!("bm25_search: {e}"));
                FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                return;
            }
        };
        FfiInitData::<Bm25State>::set(info, Bm25State { hits, next: 0 });
    }
});

quack_rs::table_scan_callback!(bm25_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<Bm25State>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        if state.next >= state.hits.len() {
            unsafe { chunk.set_size(0) };
            return;
        }
        let batch =
            (state.hits.len() - state.next).min(duckdb_vector_size() as usize);
        let mut id_writer = unsafe { chunk.writer(0) };
        let mut score_writer = unsafe { chunk.writer(1) };
        for i in 0..batch {
            let hit = &state.hits[state.next + i];
            unsafe { id_writer.write_i64(i, hit.doc_id) };
            unsafe { score_writer.write_f64(i, hit.score) };
        }
        state.next += batch;
        unsafe { chunk.set_size(batch) };
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Table function: tantivy_create_index(name, table,
//   [id_column := ''], [text_column := 'body'], [language := 'english'],
//   [path := ''])
//   → name VARCHAR, path VARCHAR, docs BIGINT, language VARCHAR
//
// Builds the persistent (on-disk, mmap-backed) index `name` from the rows of
// `table` and registers it in the process-global registry. Corpus acquisition
// follows the SAME rules as `bm25_search`: dedicated session connection (with
// the re-entry mark), the MAX_DOCS / text-byte pre-checks before any
// materialization, safe-identifier validation, NULL rows skipped. The index
// is stored under `path` (default: `$TANTIVY_INDEX_DIR/<name>`, else
// `.tantivy_index/<name>` in the current working directory); the directory is
// caller-owned and wiped before each build (rebuild semantics, like CREATE OR
// REPLACE — never a silent append). Rebuilding an already-registered name
// replaces its registry entry.
//
// bind  : validate name/table/columns/language; probe table on the session
//         connection so a missing table/column fails before any I/O.
// init  : stream the table into documents, build the on-disk index (under
//         CREATE_INDEX_LOCK), register it in the registry.
// scan  : emit the one summary row.
// ─────────────────────────────────────────────────────────────────────────────

struct CreateBind {
    name: String,
    table: String,
    id_column: String,
    text_column: String,
    language: String,
    path: String,
}

struct CreateState {
    name: String,
    path: String,
    docs: i64,
    language: String,
    /// Single-row table: emit once, then report done (DuckDB may call the
    /// scan repeatedly until a zero-sized chunk).
    done: bool,
}

quack_rs::table_bind_callback!(create_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        let name = bind_info.get_parameter_value(0).as_str().unwrap_or_default();
        let table = bind_info.get_parameter_value(1).as_str().unwrap_or_default();
        let id_column = bind_info
            .get_named_parameter_value("id_column")
            .as_str()
            .unwrap_or_default();
        let text_column = match bind_info.get_named_parameter_value("text_column").as_str() {
            Ok(s) if !s.trim().is_empty() => s,
            _ => "body".to_string(),
        };
        let language = match bind_info.get_named_parameter_value("language").as_str() {
            Ok(s) if !s.trim().is_empty() => s,
            _ => "english".to_string(),
        };
        let path = match bind_info.get_named_parameter_value("path").as_str() {
            Ok(s) if !s.trim().is_empty() => s.to_string(),
            _ => String::new(),
        };

        if !search::is_safe_index_name(&name) {
            bind_info.set_error(
                "tantivy_create_index: name must be a single identifier segment like 'my_index' or 'my-index'",
            );
            return;
        }
        if !is_safe_identifier(&table) {
            bind_info.set_error(
                "tantivy_create_index: table must be an identifier like 'docs' or 'main.docs'",
            );
            return;
        }
        let id_column = id_column.trim().to_string();
        if !id_column.is_empty() && !is_safe_identifier(&id_column) {
            bind_info.set_error(
                "tantivy_create_index: id_column must be an identifier (e.g. 'doc_id') or empty for auto ids",
            );
            return;
        }
        let text_column = text_column.trim().to_string();
        if !is_safe_identifier(&text_column) {
            bind_info
                .set_error("tantivy_create_index: text_column must be an identifier (e.g. 'body')");
            return;
        }

        // Probe table + columns on the session connection (fail early and
        // name the problem) — the same probe shape as bm25_search.
        let probe = if id_column.is_empty() {
            format!("SELECT {text_column}::VARCHAR FROM {table} LIMIT 0")
        } else {
            format!(
                "SELECT {id_column}::BIGINT, {text_column}::VARCHAR FROM {table} LIMIT 0"
            )
        };
        match session() {
            Ok(sess) => match sess.con.query(&probe) {
                Ok(_result) => {}
                Err(e) => {
                    bind_info
                        .set_error(&format!("tantivy_create_index: cannot open table '{table}': {e}"));
                    return;
                }
            },
            Err(e) => {
                bind_info.set_error(e.message());
                return;
            }
        }

        bind_info
            .add_result_column("name", TypeId::Varchar)
            .add_result_column("path", TypeId::Varchar)
            .add_result_column("docs", TypeId::BigInt)
            .add_result_column("language", TypeId::Varchar)
            .set_cardinality(1, true);
        FfiBindData::<CreateBind>::set(
            info,
            CreateBind {
                name: name.trim().to_string(),
                table: table.trim().to_string(),
                id_column,
                text_column,
                language,
                path,
            },
        );
    }
});

quack_rs::table_init_callback!(create_init, |info| {
    unsafe {
        let init_info = InitInfo::new(info);
        init_info.set_max_threads(1);

        let Some(bind) = FfiBindData::<CreateBind>::get_from_init(info) else {
            FfiInitData::<CreateState>::set(
                info,
                CreateState {
                    name: String::new(),
                    path: String::new(),
                    docs: 0,
                    language: String::new(),
                    done: true,
                },
            );
            return;
        };

        let has_id = !bind.id_column.is_empty();
        let sql = if has_id {
            format!(
                "SELECT {}::BIGINT AS id, {}::VARCHAR AS text FROM {}",
                bind.id_column, bind.text_column, bind.table
            )
        } else {
            format!("SELECT {}::VARCHAR AS text FROM {}", bind.text_column, bind.table)
        };

        let mut docs: Vec<search::Document> = Vec::new();
        let mut seq: i64 = 0;
        {
            let sess = match session() {
                Ok(s) => s,
                Err(e) => {
                    init_info.set_error(e.message());
                    FfiInitData::<CreateState>::set(
                        info,
                        CreateState {
                            name: String::new(),
                            path: String::new(),
                            docs: 0,
                            language: String::new(),
                            done: true,
                        },
                    );
                    return;
                }
            };
            // Same cheap pre-check as bm25_search: measure the corpus (rows +
            // text bytes) before copying a single row, so an oversized table
            // is rejected before the multi-GB allocation.
            let probe_sql = format!(
                "SELECT count(*)::BIGINT, COALESCE(sum(LENGTH({text})::BIGINT), 0)::BIGINT \
                 FROM {table}",
                text = bind.text_column,
                table = bind.table,
            );
            let mut p = match sess.con.query(&probe_sql) {
                Ok(r) => r,
                Err(e) => {
                    init_info.set_error(&format!("tantivy_create_index: {e}"));
                    FfiInitData::<CreateState>::set(
                        info,
                        CreateState {
                            name: String::new(),
                            path: String::new(),
                            docs: 0,
                            language: String::new(),
                            done: true,
                        },
                    );
                    return;
                }
            };
            let chunk = match p.next_chunk() {
                Ok(Some(c)) => c,
                Ok(None) => {
                    init_info.set_error("tantivy_create_index: pre-check returned no rows");
                    FfiInitData::<CreateState>::set(
                        info,
                        CreateState {
                            name: String::new(),
                            path: String::new(),
                            docs: 0,
                            language: String::new(),
                            done: true,
                        },
                    );
                    return;
                }
                Err(e) => {
                    init_info.set_error(&format!("tantivy_create_index: {e}"));
                    FfiInitData::<CreateState>::set(
                        info,
                        CreateState {
                            name: String::new(),
                            path: String::new(),
                            docs: 0,
                            language: String::new(),
                            done: true,
                        },
                    );
                    return;
                }
            };
            let r = unsafe { chunk.reader(0) };
            let c = unsafe { chunk.reader(1) };
            let row_count = unsafe { r.read_i64(0) } as u64;
            let text_len = unsafe { c.read_i64(0) } as u64;
            let text_cap = (search::MAX_DOCS * 1024) as u64; // ~1KB/doc worst case
            if row_count > search::MAX_DOCS as u64 {
                init_info.set_error(&format!(
                    "tantivy_create_index: table '{table}' has {n} rows; the limit is {d} rows — narrow the corpus with a WHERE clause or a filtered view",
                    table = bind.table,
                    n = row_count,
                    d = search::MAX_DOCS
                ));
                FfiInitData::<CreateState>::set(
                    info,
                    CreateState {
                        name: String::new(),
                        path: String::new(),
                        docs: 0,
                        language: String::new(),
                        done: true,
                    },
                );
                return;
            }
            if text_len > text_cap {
                init_info.set_error(&format!(
                    "tantivy_create_index: table '{table}' holds {t} bytes of text across {n} rows; the limit is {cap} bytes — narrow the corpus",
                    table = bind.table,
                    t = text_len,
                    n = row_count,
                    cap = text_cap
                ));
                FfiInitData::<CreateState>::set(
                    info,
                    CreateState {
                        name: String::new(),
                        path: String::new(),
                        docs: 0,
                        language: String::new(),
                        done: true,
                    },
                );
                return;
            }
            let mut result = match sess.con.query(&sql) {
                Ok(r) => r,
                Err(e) => {
                    init_info.set_error(&format!("tantivy_create_index: {e}"));
                    FfiInitData::<CreateState>::set(
                        info,
                        CreateState {
                            name: String::new(),
                            path: String::new(),
                            docs: 0,
                            language: String::new(),
                            done: true,
                        },
                    );
                    return;
                }
            };
            loop {
                match result.next_chunk() {
                    Ok(Some(chunk)) => {
                        let d: &DataChunk = &chunk;
                        let text_reader = unsafe { d.reader(if has_id { 1 } else { 0 }) };
                        let id_reader = has_id.then(|| unsafe { d.reader(0) });
                        for row in 0..d.size() {
                            if !unsafe { text_reader.is_valid(row) } {
                                continue; // NULL/missing text → nothing to index
                            }
                            let text = unsafe { text_reader.read_str(row) }.to_string();
                            if text.len() > search::MAX_TEXT_BYTES {
                                init_info.set_error(&format!(
                                    "tantivy_create_index: a row in '{table}' exceeds the {b}-byte per-text limit",
                                    table = bind.table,
                                    b = search::MAX_TEXT_BYTES
                                ));
                                FfiInitData::<CreateState>::set(
                                    info,
                                    CreateState {
                                        name: String::new(),
                                        path: String::new(),
                                        docs: 0,
                                        language: String::new(),
                                        done: true,
                                    },
                                );
                                return;
                            }
                            let id = match &id_reader {
                                Some(r) => {
                                    if !unsafe { r.is_valid(row) } {
                                        continue;
                                    }
                                    unsafe { r.read_i64(row) }
                                }
                                None => {
                                    let id = seq;
                                    seq += 1;
                                    id
                                }
                            };
                            docs.push(search::Document { id, text });
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        init_info.set_error(&format!("tantivy_create_index: {e}"));
                        FfiInitData::<CreateState>::set(
                            info,
                            CreateState {
                                name: String::new(),
                                path: String::new(),
                                docs: 0,
                                language: String::new(),
                                done: true,
                            },
                        );
                        return;
                    }
                }
            }
            // `result`, `p`, and `sess` (session lock + re-entry mark) drop
            // here, before the lock-free build below.
        }

        let dir = if bind.path.trim().is_empty() {
            search::persistent_index_dir(&bind.name)
        } else {
            std::path::PathBuf::from(bind.path.trim())
        };
        // Serialize builds: a concurrent create/load of the same name must
        // not interleave wipe/write in the index directory.
        let _build_guard =
            CREATE_INDEX_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let idx = match search::create_persistent(&bind.name, &dir, &bind.language, &docs) {
            Ok(i) => i,
            Err(e) => {
                init_info.set_error(&format!("tantivy_create_index: {e}"));
                FfiInitData::<CreateState>::set(
                    info,
                    CreateState {
                        name: String::new(),
                        path: String::new(),
                        docs: 0,
                        language: String::new(),
                        done: true,
                    },
                );
                return;
            }
        };
        let out = CreateState {
            name: idx.name.clone(),
            path: idx.path.clone(),
            docs: idx.num_docs as i64,
            language: idx.language.clone(),
            done: false,
        };
        // Register: a rebuild replaces any previous entry under this name.
        registry_lock().insert(out.name.clone(), Arc::new(idx));

        FfiInitData::<CreateState>::set(info, out);
    }
});

quack_rs::table_scan_callback!(create_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<CreateState>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        if state.done {
            unsafe { chunk.set_size(0) };
            return;
        }
        let mut name_writer = unsafe { chunk.writer(0) };
        let mut path_writer = unsafe { chunk.writer(1) };
        let mut docs_writer = unsafe { chunk.writer(2) };
        let mut lang_writer = unsafe { chunk.writer(3) };
        unsafe {
            name_writer.write_varchar(0, &state.name);
            path_writer.write_varchar(0, &state.path);
            docs_writer.write_i64(0, state.docs);
            lang_writer.write_varchar(0, &state.language);
        }
        state.done = true;
        unsafe { chunk.set_size(1) };
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Table function: tantivy_search_index(name, query, [top_k := 10])
//   → doc_id BIGINT, score DOUBLE
//
// BM25 search against a PERSISTENT index registered under `name` (by
// `tantivy_create_index` or `tantivy_load_index`). Query semantics are exactly
// `bm25_search`'s (NFC + metacharacter sanitization, conjunction/AND, and the
// full-match-set tie-break honored at the cutoff), so both functions agree
// bit-for-bit on the same corpus + parameters. No session connection is
// needed — the index is on disk, not in a table.
//
// bind  : validate `name` (safe identifier, registered) and `top_k`.
// init  : clone the index handle out of the registry (lock held only for the
//         clone), run the search lock-free, keep the ranked hits.
// scan  : emit the hits in batches (identical shape to bm25_search).
// ─────────────────────────────────────────────────────────────────────────────

struct PSearchBind {
    name: String,
    query: String,
    limit: i64,
}

struct PSearchState {
    hits: Vec<search::Hit>,
    next: usize,
}

quack_rs::table_bind_callback!(psearch_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        let name = bind_info.get_parameter_value(0).as_str().unwrap_or_default();
        let query = bind_info.get_parameter_value(1).as_str().unwrap_or_default();

        // `top_k` defaults to 10 (the persistent-search default) and shares
        // bm25_search's [1, 100_000] bounds: below 1 is a semantic error, and
        // above the cap the full-match-set collector allocation grows without
        // bound. Out-of-range values are rejected, never clamped.
        let limit = match bind_info.get_named_parameter_value("top_k").as_i64() {
            Some(v) if (1..=100_000).contains(&v) => v,
            Some(v) => {
                bind_info.set_error(&format!(
                    "tantivy_search_index: top_k must be between 1 and 100000 (got {v})"
                ));
                return;
            }
            None => 10,
        };

        if !search::is_safe_index_name(&name) {
            bind_info.set_error(
                "tantivy_search_index: name must be a single identifier segment like 'my_index' or 'my-index'",
            );
            return;
        }
        // Bind-time existence check: a missing index is a clear, early error
        // (naming the two ways to create one) rather than a surprise at init.
        let registered = registry_lock().contains_key(&name);
        if !registered {
            bind_info.set_error(&format!(
                "tantivy_search_index: no persistent index '{name}' is registered; \
                 create it with tantivy_create_index or tantivy_load_index \
                 (see tantivy_list() for registered names)"
            ));
            return;
        }

        bind_info
            .add_result_column("doc_id", TypeId::BigInt)
            .add_result_column("score", TypeId::Double)
            .set_cardinality(limit as u64, false);
        FfiBindData::<PSearchBind>::set(
            info,
            PSearchBind {
                name: name.trim().to_string(),
                query,
                limit,
            },
        );
    }
});

quack_rs::table_init_callback!(psearch_init, |info| {
    unsafe {
        let init_info = InitInfo::new(info);
        init_info.set_max_threads(1);

        let Some(bind) = FfiBindData::<PSearchBind>::get_from_init(info) else {
            FfiInitData::<PSearchState>::set(info, PSearchState { hits: Vec::new(), next: 0 });
            return;
        };

        // Clone the Arc out of the registry and release the lock before the
        // search (lock discipline: the registry lock is never held across a
        // query). A concurrent rebuild between bind and init is possible in
        // principle; re-checking here keeps the failure explicit.
        let idx: Arc<search::PersistentIndex> = match registry_lock().get(&bind.name).cloned() {
            Some(i) => i,
            None => {
                init_info.set_error(&format!(
                    "tantivy_search_index: no persistent index '{}' is registered",
                    bind.name
                ));
                FfiInitData::<PSearchState>::set(info, PSearchState { hits: Vec::new(), next: 0 });
                return;
            }
        };

        let hits = match search::persistent_search(&idx, &bind.query, bind.limit as usize) {
            Ok(h) => h,
            Err(e) => {
                init_info.set_error(&format!("tantivy_search_index: {e}"));
                FfiInitData::<PSearchState>::set(info, PSearchState { hits: Vec::new(), next: 0 });
                return;
            }
        };
        FfiInitData::<PSearchState>::set(info, PSearchState { hits, next: 0 });
    }
});

quack_rs::table_scan_callback!(psearch_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<PSearchState>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        if state.next >= state.hits.len() {
            unsafe { chunk.set_size(0) };
            return;
        }
        let batch = (state.hits.len() - state.next).min(duckdb_vector_size() as usize);
        let mut id_writer = unsafe { chunk.writer(0) };
        let mut score_writer = unsafe { chunk.writer(1) };
        for i in 0..batch {
            let hit = &state.hits[state.next + i];
            unsafe { id_writer.write_i64(i, hit.doc_id) };
            unsafe { score_writer.write_f64(i, hit.score) };
        }
        state.next += batch;
        unsafe { chunk.set_size(batch) };
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Table function: tantivy_list()
//   → name VARCHAR, path VARCHAR, docs BIGINT, language VARCHAR
//
// A snapshot of the process-global registry of persistent indexes: one row
// per registered index, sorted by name. The snapshot is taken at init time
// (the registry lock is held only while copying the fields out), so a
// concurrent build/load cannot mutate the rows mid-scan.
// ─────────────────────────────────────────────────────────────────────────────

struct ListState {
    rows: Vec<(String, String, i64, String)>,
    next: usize,
}

quack_rs::table_bind_callback!(list_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        bind_info
            .add_result_column("name", TypeId::Varchar)
            .add_result_column("path", TypeId::Varchar)
            .add_result_column("docs", TypeId::BigInt)
            .add_result_column("language", TypeId::Varchar)
            .set_cardinality(0, false); // exact size unknown until the snapshot
    }
});

quack_rs::table_init_callback!(list_init, |info| {
    unsafe {
        InitInfo::new(info).set_max_threads(1);
        // Snapshot the registry (short lock, plain field copies out).
        let mut rows: Vec<(String, String, i64, String)> = registry_lock()
            .iter()
            .map(|(name, idx)| {
                (
                    name.clone(),
                    idx.path.clone(),
                    idx.num_docs as i64,
                    idx.language.clone(),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        FfiInitData::<ListState>::set(info, ListState { rows, next: 0 });
    }
});

quack_rs::table_scan_callback!(list_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<ListState>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        if state.next >= state.rows.len() {
            unsafe { chunk.set_size(0) };
            return;
        }
        let batch = (state.rows.len() - state.next).min(duckdb_vector_size() as usize);
        let mut name_writer = unsafe { chunk.writer(0) };
        let mut path_writer = unsafe { chunk.writer(1) };
        let mut docs_writer = unsafe { chunk.writer(2) };
        let mut lang_writer = unsafe { chunk.writer(3) };
        for i in 0..batch {
            let row = &state.rows[state.next + i];
            unsafe {
                name_writer.write_varchar(i, &row.0);
                path_writer.write_varchar(i, &row.1);
                docs_writer.write_i64(i, row.2);
                lang_writer.write_varchar(i, &row.3);
            }
        }
        state.next += batch;
        unsafe { chunk.set_size(batch) }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Table function: supported_languages() → lang VARCHAR
// (19 rows: 18 Snowball languages + `chinese (jieba)`, see language_display)
// ─────────────────────────────────────────────────────────────────────────────

struct LangState {
    next: usize,
}

quack_rs::table_bind_callback!(langs_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        bind_info
            .add_result_column("lang", TypeId::Varchar)
            .set_cardinality(search::SUPPORTED_LANGUAGES.len() as u64, true);
    }
});

quack_rs::table_init_callback!(langs_init, |info| {
    unsafe {
        InitInfo::new(info).set_max_threads(1);
        FfiInitData::<LangState>::set(info, LangState { next: 0 });
    }
});

quack_rs::table_scan_callback!(langs_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<LangState>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        let langs = search::SUPPORTED_LANGUAGES;
        if state.next >= langs.len() {
            unsafe { chunk.set_size(0) };
            return;
        }
        let batch = (langs.len() - state.next).min(duckdb_vector_size() as usize);
        let mut writer = unsafe { chunk.writer(0) };
        for i in 0..batch {
            unsafe { writer.write_varchar(i, search::language_display(langs[state.next + i])) };
        }
        state.next += batch;
        unsafe { chunk.set_size(batch) }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Table function: tantivy_terms(table, text_column := 'body')
//   → term VARCHAR, df BIGINT
//
// The raw-term (surface-form) dictionary of a table's text column: every
// lowercased, unstemmed unicode word with its document frequency, sorted by
// (df DESC, term ASC). This is the correction-space dictionary for the typo
// tolerance macro layer (the fts `raw_dict` equivalent) — edit-distance
// correction must run against surface spellings, not stems.
//
// Ephemeral like `bm25_search`: the dictionary is built in RAM on each call
// from the table's current rows, with the same MAX_DOCS / text-byte guards.
// ─────────────────────────────────────────────────────────────────────────────

struct TermsBind {
    table: String,
    text_column: String,
}

struct TermsState {
    terms: Vec<search::RawTerm>,
    next: usize,
}

quack_rs::table_bind_callback!(terms_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        let table = bind_info.get_parameter_value(0).as_str().unwrap_or_default();
        let text_column = match bind_info.get_named_parameter_value("text_column").as_str() {
            Ok(s) if !s.trim().is_empty() => s,
            _ => "body".to_string(),
        };

        if !is_safe_identifier(&table) {
            bind_info
                .set_error("tantivy_terms: table_name must be an identifier like 'docs' or 'main.docs'");
            return;
        }
        let text_column = text_column.trim().to_string();
        if !is_safe_identifier(&text_column) {
            bind_info.set_error("tantivy_terms: text_column must be an identifier (e.g. 'body')");
            return;
        }

        let probe = format!("SELECT {text_column}::VARCHAR FROM {table} LIMIT 0");
        match session() {
            Ok(sess) => match sess.con.query(&probe) {
                Ok(_result) => {}
                Err(e) => {
                    bind_info
                        .set_error(&format!("tantivy_terms: cannot open table '{table}': {e}"));
                    return;
                }
            },
            Err(e) => {
                bind_info.set_error(e.message());
                return;
            }
        }

        bind_info
            .add_result_column("term", TypeId::Varchar)
            .add_result_column("df", TypeId::BigInt)
            .set_cardinality(0, false); // exact size unknown until the scan; unknown
        FfiBindData::<TermsBind>::set(
            info,
            TermsBind {
                table: table.trim().to_string(),
                text_column,
            },
        );
    }
});

quack_rs::table_init_callback!(terms_init, |info| {
    unsafe {
        let init_info = InitInfo::new(info);
        init_info.set_max_threads(1);

        let Some(bind) = FfiBindData::<TermsBind>::get_from_init(info) else {
            FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
            return;
        };

        let sql = format!(
            "SELECT {text}::VARCHAR AS text FROM {table}",
            text = bind.text_column,
            table = bind.table
        );

        let mut texts: Vec<String> = Vec::new();
        {
            let sess = match session() {
                Ok(s) => s,
                Err(e) => {
                    init_info.set_error(e.message());
                    FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                    return;
                }
            };
            // Same cheap pre-check as bm25_search: measure before materializing.
            let probe_sql = format!(
                "SELECT count(*)::BIGINT, COALESCE(sum(LENGTH({text})::BIGINT), 0)::BIGINT \
                 FROM {table}",
                text = bind.text_column,
                table = bind.table,
            );
            let mut p = match sess.con.query(&probe_sql) {
                Ok(r) => r,
                Err(e) => {
                    init_info.set_error(&format!("tantivy_terms: {e}"));
                    FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                    return;
                }
            };
            let chunk = match p.next_chunk() {
                Ok(Some(c)) => c,
                Ok(None) => {
                    init_info.set_error("tantivy_terms: pre-check returned no rows");
                    FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                    return;
                }
                Err(e) => {
                    init_info.set_error(&format!("tantivy_terms: {e}"));
                    FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                    return;
                }
            };
            let r = unsafe { chunk.reader(0) };
            let c = unsafe { chunk.reader(1) };
            let row_count = unsafe { r.read_i64(0) } as u64;
            let text_len = unsafe { c.read_i64(0) } as u64;
            let text_cap = (search::MAX_DOCS * 1024) as u64;
            if row_count > search::MAX_DOCS as u64 {
                init_info.set_error(&format!(
                    "tantivy_terms: table '{table}' has {n} rows; the limit is {d} rows — narrow the corpus with a WHERE clause or a filtered view",
                    table = bind.table,
                    n = row_count,
                    d = search::MAX_DOCS
                ));
                FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                return;
            }
            if text_len > text_cap {
                init_info.set_error(&format!(
                    "tantivy_terms: table '{table}' holds {t} bytes of text across {n} rows; the limit is {cap} bytes — narrow the corpus",
                    table = bind.table,
                    t = text_len,
                    n = row_count,
                    cap = text_cap
                ));
                FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                return;
            }
            let mut result = match sess.con.query(&sql) {
                Ok(r) => r,
                Err(e) => {
                    init_info.set_error(&format!("tantivy_terms: {e}"));
                    FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                    return;
                }
            };
            loop {
                match result.next_chunk() {
                    Ok(Some(chunk)) => {
                        let d: &DataChunk = &chunk;
                        let text_reader = unsafe { d.reader(0) };
                        for row in 0..d.size() {
                            if !unsafe { text_reader.is_valid(row) } {
                                continue; // NULL/missing text → skip
                            }
                            let text = unsafe { text_reader.read_str(row) }.to_string();
                            if text.len() > search::MAX_TEXT_BYTES {
                                init_info.set_error(&format!(
                                    "tantivy_terms: a row in '{table}' exceeds the {b}-byte per-text limit",
                                    table = bind.table,
                                    b = search::MAX_TEXT_BYTES
                                ));
                                FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                                return;
                            }
                            texts.push(text);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        init_info.set_error(&format!("tantivy_terms: {e}"));
                        FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                        return;
                    }
                }
            }
            // `result`, `p`, and `sess` (session lock + re-entry mark) drop here.
        }

        let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let terms = match search::raw_term_dictionary(&refs) {
            Ok(t) => t,
            Err(e) => {
                init_info.set_error(&format!("tantivy_terms: {e}"));
                FfiInitData::<TermsState>::set(info, TermsState { terms: Vec::new(), next: 0 });
                return;
            }
        };
        FfiInitData::<TermsState>::set(info, TermsState { terms, next: 0 });
    }
});

quack_rs::table_scan_callback!(terms_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<TermsState>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        if state.next >= state.terms.len() {
            unsafe { chunk.set_size(0) };
            return;
        }
        let batch = (state.terms.len() - state.next).min(duckdb_vector_size() as usize);
        let mut term_writer = unsafe { chunk.writer(0) };
        let mut df_writer = unsafe { chunk.writer(1) };
        for i in 0..batch {
            let t = &state.terms[state.next + i];
            unsafe {
                term_writer.write_varchar(i, t.term.as_str());
                df_writer.write_i64(i, t.df);
            }
        }
        state.next += batch;
        unsafe { chunk.set_size(batch) }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Table function: tantivy_save_index(name, [path := ''])
//   → name VARCHAR, path VARCHAR, docs BIGINT
//
// Copies the directory of the registered index `name` to `path`, or (when
// `path` is omitted) simply reports where the index currently lives. The copy
// is a full recursive copy of every file in the index directory (including
// the `__tantivy_meta.json` provenance sidecar), so the destination is a
// complete, loadable index: `tantivy_load_index` works on it from any other
// process. Guards: the destination must not be an ancestor/descendant of the
// source (recursion hazard), and if it exists it must be EMPTY (never a
// silent overwrite). The running index keeps reading from its original
// location — save is a backup/transfer primitive, not a move.
// ─────────────────────────────────────────────────────────────────────────────

/// True if `desc` is `anc` itself or nested inside it.
fn is_ancestor_or_self(anc: &std::path::Path, desc: &std::path::Path) -> bool {
    if anc == desc {
        return true;
    }
    let mut cur = desc;
    loop {
        match cur.parent() {
            Some(p) if p == anc => return true,
            Some(p) => cur = p,
            None => return false,
        }
    }
}

/// Recursively copy every entry of `src` into `dst` (which must exist and be
/// empty; nested directories are created as needed).
fn copy_index_dir(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(src)? {
        let e = entry?;
        let target = dst.join(e.file_name());
        if e.path().is_dir() {
            std::fs::create_dir_all(&target)?;
            copy_index_dir(&e.path(), &target)?;
        } else {
            std::fs::copy(e.path(), &target)?;
        }
    }
    Ok(())
}

struct SaveBind {
    name: String,
    path: String,
}

struct SaveState {
    name: String,
    path: String,
    docs: i64,
    done: bool,
}

quack_rs::table_bind_callback!(save_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        let name = bind_info.get_parameter_value(0).as_str().unwrap_or_default();
        let path = match bind_info.get_named_parameter_value("path").as_str() {
            Ok(s) if !s.trim().is_empty() => s.to_string(),
            _ => String::new(),
        };

        if !search::is_safe_index_name(&name) {
            bind_info.set_error(
                "tantivy_save_index: name must be a single identifier segment like 'my_index' or 'my-index'",
            );
            return;
        }
        let registered = registry_lock().contains_key(&name);
        if !registered {
            bind_info.set_error(&format!(
                "tantivy_save_index: no persistent index '{name}' is registered; \
                 create it with tantivy_create_index or tantivy_load_index \
                 (see tantivy_list() for registered names)"
            ));
            return;
        }

        bind_info
            .add_result_column("name", TypeId::Varchar)
            .add_result_column("path", TypeId::Varchar)
            .add_result_column("docs", TypeId::BigInt)
            .set_cardinality(1, true);
        FfiBindData::<SaveBind>::set(info, SaveBind { name: name.trim().to_string(), path });
    }
});

quack_rs::table_init_callback!(save_init, |info| {
    unsafe {
        let init_info = InitInfo::new(info);
        init_info.set_max_threads(1);

        let Some(bind) = FfiBindData::<SaveBind>::get_from_init(info) else {
            FfiInitData::<SaveState>::set(
                info,
                SaveState { name: String::new(), path: String::new(), docs: 0, done: true },
            );
            return;
        };

        let idx: Arc<search::PersistentIndex> = match registry_lock().get(&bind.name).cloned() {
            Some(i) => i,
            None => {
                init_info.set_error(&format!(
                    "tantivy_save_index: no persistent index '{}' is registered",
                    bind.name
                ));
                FfiInitData::<SaveState>::set(
                    info,
                    SaveState { name: String::new(), path: String::new(), docs: 0, done: true },
                );
                return;
            }
        };

        let src = std::path::Path::new(&idx.path);
        let dst = if bind.path.trim().is_empty() {
            src.to_path_buf() // no path given: just report where it lives
        } else {
            std::path::PathBuf::from(bind.path.trim())
        };

        if dst != src {
            // Recursion hazard: never copy a tree into (or onto) itself.
            if is_ancestor_or_self(&dst, src) || is_ancestor_or_self(src, &dst) {
                init_info.set_error(&format!(
                    "tantivy_save_index: destination '{}' must not be inside (or contain) the index directory '{}'",
                    dst.display(), src.display()
                ));
                FfiInitData::<SaveState>::set(
                    info,
                    SaveState { name: String::new(), path: String::new(), docs: 0, done: true },
                );
                return;
            }
            // Never overwrite: an existing destination must be empty.
            if dst.exists() {
                let non_empty = match std::fs::read_dir(&dst) {
                    Ok(mut it) => it.next().is_some(),
                    Err(e) => {
                        init_info.set_error(&format!(
                            "tantivy_save_index: destination '{}' is not a directory: {e}",
                            dst.display()
                        ));
                        FfiInitData::<SaveState>::set(
                            info,
                            SaveState { name: String::new(), path: String::new(), docs: 0, done: true },
                        );
                        return;
                    }
                };
                if non_empty {
                    init_info.set_error(&format!(
                        "tantivy_save_index: destination '{}' exists and is not empty; refusing to overwrite",
                        dst.display()
                    ));
                    FfiInitData::<SaveState>::set(
                        info,
                        SaveState { name: String::new(), path: String::new(), docs: 0, done: true },
                    );
                    return;
                }
            }
            if let Err(e) = std::fs::create_dir_all(&dst) {
                init_info.set_error(&format!(
                    "tantivy_save_index: cannot create destination '{}': {e}",
                    dst.display()
                ));
                FfiInitData::<SaveState>::set(
                    info,
                    SaveState { name: String::new(), path: String::new(), docs: 0, done: true },
                );
                return;
            }
            if let Err(e) = copy_index_dir(src, &dst) {
                init_info.set_error(&format!(
                    "tantivy_save_index: cannot copy '{}' → '{}': {e}",
                    src.display(), dst.display()
                ));
                FfiInitData::<SaveState>::set(
                    info,
                    SaveState { name: String::new(), path: String::new(), docs: 0, done: true },
                );
                return;
            }
        }

        FfiInitData::<SaveState>::set(
            info,
            SaveState {
                name: bind.name.clone(),
                path: dst.to_string_lossy().into_owned(),
                docs: idx.num_docs as i64,
                done: false,
            },
        );
    }
});

quack_rs::table_scan_callback!(save_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<SaveState>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        if state.done {
            unsafe { chunk.set_size(0) };
            return;
        }
        let mut name_writer = unsafe { chunk.writer(0) };
        let mut path_writer = unsafe { chunk.writer(1) };
        let mut docs_writer = unsafe { chunk.writer(2) };
        unsafe {
            name_writer.write_varchar(0, &state.name);
            path_writer.write_varchar(0, &state.path);
            docs_writer.write_i64(0, state.docs);
        }
        state.done = true;
        unsafe { chunk.set_size(1) };
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Table function: tantivy_load_index(name, path)
//   → name VARCHAR, path VARCHAR, docs BIGINT, language VARCHAR
//
// Opens the persistent index stored at `path` (a directory produced by
// `tantivy_create_index` / `tantivy_save_index` in any process) read-only and
// registers it under `name` in THIS process's registry. The registry is
// process-global, so `name` must not already be registered here (clear error;
// v0.3 has no drop function — replace an index by rebuilding it under a fresh
// name or in a fresh process).
// ─────────────────────────────────────────────────────────────────────────────

struct LoadBind {
    name: String,
    path: String,
}

struct LoadState {
    name: String,
    path: String,
    docs: i64,
    language: String,
    done: bool,
}

quack_rs::table_bind_callback!(load_bind, |info| {
    unsafe {
        let bind_info = BindInfo::new(info);
        let name = bind_info.get_parameter_value(0).as_str().unwrap_or_default();
        let path = bind_info.get_parameter_value(1).as_str().unwrap_or_default();

        if !search::is_safe_index_name(&name) {
            bind_info.set_error(
                "tantivy_load_index: name must be a single identifier segment like 'my_index' or 'my-index'",
            );
            return;
        }
        if path.trim().is_empty() {
            bind_info.set_error(
                "tantivy_load_index: path (the index directory) is required",
            );
            return;
        }
        let registered = registry_lock().contains_key(&name);
        if registered {
            bind_info.set_error(&format!(
                "tantivy_load_index: index '{name}' is already registered in this process; \
                 v0.3 has no drop — use a different name (or a fresh process)"
            ));
            return;
        }

        bind_info
            .add_result_column("name", TypeId::Varchar)
            .add_result_column("path", TypeId::Varchar)
            .add_result_column("docs", TypeId::BigInt)
            .add_result_column("language", TypeId::Varchar)
            .set_cardinality(1, true);
        FfiBindData::<LoadBind>::set(
            info,
            LoadBind {
                name: name.trim().to_string(),
                path: path.trim().to_string(),
            },
        );
    }
});

quack_rs::table_init_callback!(load_init, |info| {
    unsafe {
        let init_info = InitInfo::new(info);
        init_info.set_max_threads(1);

        let Some(bind) = FfiBindData::<LoadBind>::get_from_init(info) else {
            FfiInitData::<LoadState>::set(
                info,
                LoadState {
                    name: String::new(),
                    path: String::new(),
                    docs: 0,
                    language: String::new(),
                    done: true,
                },
            );
            return;
        };

        let idx = match search::open_persistent(&bind.name, std::path::Path::new(&bind.path)) {
            Ok(i) => i,
            Err(e) => {
                init_info.set_error(&format!("tantivy_load_index: {e}"));
                FfiInitData::<LoadState>::set(
                    info,
                    LoadState {
                        name: String::new(),
                        path: String::new(),
                        docs: 0,
                        language: String::new(),
                        done: true,
                    },
                );
                return;
            }
        };
        let out = LoadState {
            name: idx.name.clone(),
            path: idx.path.clone(),
            docs: idx.num_docs as i64,
            language: idx.language.clone(),
            done: false,
        };
        // Re-check + insert: a concurrent load of the same name must lose
        // with a clear error, never a silent duplicate/overwrite.
        match registry_lock().insert(out.name.clone(), Arc::new(idx)) {
            Some(_prev) => {
                init_info.set_error(&format!(
                    "tantivy_load_index: index '{}' was registered concurrently; it now points at the newer load",
                    out.name
                ));
            }
            None => {}
        }

        FfiInitData::<LoadState>::set(info, out);
    }
});

quack_rs::table_scan_callback!(load_scan, |info, output| {
    unsafe {
        let chunk = DataChunk::from_raw(output);
        let Some(state) = FfiInitData::<LoadState>::get_mut(info) else {
            unsafe { chunk.set_size(0) };
            return;
        };
        if state.done {
            unsafe { chunk.set_size(0) };
            return;
        }
        let mut name_writer = unsafe { chunk.writer(0) };
        let mut path_writer = unsafe { chunk.writer(1) };
        let mut docs_writer = unsafe { chunk.writer(2) };
        let mut lang_writer = unsafe { chunk.writer(3) };
        unsafe {
            name_writer.write_varchar(0, &state.name);
            path_writer.write_varchar(0, &state.path);
            docs_writer.write_i64(0, state.docs);
            lang_writer.write_varchar(0, &state.language);
        }
        state.done = true;
        unsafe { chunk.set_size(1) };
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Scalar: tokenize(VARCHAR) → VARCHAR[], tokenize(VARCHAR, VARCHAR) → VARCHAR[]
// ─────────────────────────────────────────────────────────────────────────────

quack_rs::scalar_callback!(tokenize_default_fn, |info, input, output| {
    unsafe {
        let reader = VectorReader::new(input, 0);
        let row_count = reader.row_count();
        let fn_info = ScalarFunctionInfo::new(info);
        let mut builder = ListBuilder::new(output).with_element_limit(1_000_000);
        for row in 0..row_count {
            if !reader.is_valid(row) {
                builder.push_row(row, 0, |_, _| {});
                VectorWriter::from_vector(output).set_null(row);
                continue;
            }
            let text = reader.read_str(row);
            match search::tokenize_default(text) {
                Ok(toks) => {
                    let n = toks.len();
                    let toks = toks;
                    builder.push_row(row, n, |w, base| {
                        for (i, t) in toks.iter().enumerate() {
                            w.write_varchar(base + i, t);
                        }
                    });
                }
                Err(e) => {
                    fn_info.set_error(&format!("tokenize: {e}"));
                    return;
                }
            }
        }
        // P1-2: overflow must be an error, never a silent NULL row.
        // (checked before finish(), which consumes the builder)
        if builder.overflowed() {
            fn_info.set_error("tokenize: total output exceeds 1,000,000 elements per call; process smaller batches");
        }
        builder.finish();
    }
});

quack_rs::scalar_callback!(tokenize_lang_fn, |info, input, output| {
    unsafe {
        let text_reader = VectorReader::new(input, 0);
        let lang_reader = VectorReader::new(input, 1);
        let row_count = text_reader.row_count();
        let fn_info = ScalarFunctionInfo::new(info);
        let mut builder = ListBuilder::new(output).with_element_limit(1_000_000);
        for row in 0..row_count {
            if !text_reader.is_valid(row) {
                builder.push_row(row, 0, |_, _| {});
                VectorWriter::from_vector(output).set_null(row);
                continue;
            }
            let text = text_reader.read_str(row);
            let lang = if lang_reader.is_valid(row) {
                let l = lang_reader.read_str(row);
                if l.trim().is_empty() {
                    "english".to_string()
                } else {
                    l.to_string()
                }
            } else {
                "english".to_string()
            };
            match search::tokenize_lang(text, &lang) {
                Ok(toks) => {
                    let n = toks.len();
                    let toks = toks;
                    builder.push_row(row, n, |w, base| {
                        for (i, t) in toks.iter().enumerate() {
                            w.write_varchar(base + i, t);
                        }
                    });
                }
                Err(e) => {
                    fn_info.set_error(&format!("tokenize: {e}"));
                    return;
                }
            }
        }
        // P1-2: overflow must be an error, never a silent NULL row.
        // (checked before finish(), which consumes the builder)
        if builder.overflowed() {
            fn_info.set_error("tokenize: total output exceeds 1,000,000 elements per call; process smaller batches");
        }
        builder.finish();
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Scalar: stem(VARCHAR, VARCHAR) → VARCHAR
// ─────────────────────────────────────────────────────────────────────────────

quack_rs::scalar_callback!(stem_fn, |info, input, output| {
    unsafe {
        let word_reader = VectorReader::new(input, 0);
        let lang_reader = VectorReader::new(input, 1);
        let row_count = word_reader.row_count();
        let mut writer = VectorWriter::new(output);
        let fn_info = ScalarFunctionInfo::new(info);
        for row in 0..row_count {
            if !word_reader.is_valid(row) || !lang_reader.is_valid(row) {
                writer.set_null(row);
                continue;
            }
            let word = word_reader.read_str(row);
            let lang = lang_reader.read_str(row);
            let lang = if lang.trim().is_empty() { "english" } else { lang };
            match search::stem(word, lang) {
                Ok(s) => writer.write_varchar(row, &s),
                Err(e) => {
                    fn_info.set_error(&format!("stem: {e}"));
                    return;
                }
            }
        }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Scalar: bm25_score(doc, query[, language]) → DOUBLE
// ─────────────────────────────────────────────────────────────────────────────

quack_rs::scalar_callback!(bm25_score_2_fn, |info, input, output| {
    unsafe {
        let doc_reader = VectorReader::new(input, 0);
        let query_reader = VectorReader::new(input, 1);
        let row_count = doc_reader.row_count();
        let mut writer = VectorWriter::new(output);
        let fn_info = ScalarFunctionInfo::new(info);
        for row in 0..row_count {
            if !doc_reader.is_valid(row) || !query_reader.is_valid(row) {
                writer.set_null(row);
                continue;
            }
            let doc = doc_reader.read_str(row);
            let query = query_reader.read_str(row);
            match search::bm25_score(doc, query, "english") {
                Ok(s) => writer.write_f64(row, s),
                Err(e) => {
                    fn_info.set_error(&format!("bm25_score: {e}"));
                    return;
                }
            }
        }
    }
});

quack_rs::scalar_callback!(bm25_score_3_fn, |info, input, output| {
    unsafe {
        let doc_reader = VectorReader::new(input, 0);
        let query_reader = VectorReader::new(input, 1);
        let lang_reader = VectorReader::new(input, 2);
        let row_count = doc_reader.row_count();
        let mut writer = VectorWriter::new(output);
        let fn_info = ScalarFunctionInfo::new(info);
        for row in 0..row_count {
            if !doc_reader.is_valid(row) || !query_reader.is_valid(row) {
                writer.set_null(row);
                continue;
            }
            let doc = doc_reader.read_str(row);
            let query = query_reader.read_str(row);
            let lang = if lang_reader.is_valid(row) {
                let l = lang_reader.read_str(row);
                if l.trim().is_empty() {
                    "english".to_string()
                } else {
                    l.to_string()
                }
            } else {
                "english".to_string()
            };
            match search::bm25_score(doc, query, &lang) {
                Ok(s) => writer.write_f64(row, s),
                Err(e) => {
                    fn_info.set_error(&format!("bm25_score: {e}"));
                    return;
                }
            }
        }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Scalar: tantivy_version() → VARCHAR (zero-arg)
// ─────────────────────────────────────────────────────────────────────────────

quack_rs::scalar_callback!(tantivy_version_fn, |_info, input, output| {
    unsafe {
        let row_count = duckdb_data_chunk_get_size(input) as usize;
        let mut writer = VectorWriter::new(output);
        let version = search::tantivy_version();
        for row in 0..row_count {
            writer.write_varchar(row, &version);
        }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Registration + entry point
// ─────────────────────────────────────────────────────────────────────────────

unsafe fn register_all(con: &Connection) -> Result<(), ExtensionError> {
    // Bind the dedicated session connection to the FIRST database that loads
    // this extension, and make a *second, different* database loud (see the
    // module note). `con.as_raw_database()` is the `duckdb_database` this
    // registration belongs to; distinct databases yield distinct non-null
    // pointers, so pointer identity is a sound database identity here.
    let db = con.as_raw_database() as *mut ();
    match FIRST_DB.load(Ordering::SeqCst) {
        null if null.is_null() => {
            // First loader in this process: open the dedicated connection from
            // its database and claim the identity slot. A compare-exchange
            // keeps the slot for whoever wins the race; a loser leaves the
            // connection it already opened (the single-DB check below still
            // catches a genuinely different second database).
            let won = FIRST_DB.compare_exchange(
                std::ptr::null_mut(),
                db,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            if won.is_ok() {
                // We are the first loader: our connection owns the session.
                let owned = con.open_connection()?;
                if SESSION.set(Mutex::new(owned)).is_err() {
                    // Another thread won the SESSION race (it also loaded us
                    // first); drop our now-redundant handle.
                }
            }
            // If the compare-exchange lost, another thread already claimed
            // the first-database identity with its own `db`; our `db` here is
            // either the same database (single-DB case, fine) or a second
            // database, which the check below would have caught if it had
            // run after the claim — handle it explicitly for the loser path.
            else {
                let first = won.unwrap_err();
                if db != first && !db.is_null() {
                    MULTI_DB.store(true, Ordering::SeqCst);
                }
            }
        }
        first => {
            // A database already loaded the extension. If this is a *different*
            // database, we cannot attribute `bm25_search` to the right one (the
            // C API gives a callback no way back to its database), so flag
            // multi-database and let every search fail loudly rather than read
            // the first database's tables.
            if db != first && !db.is_null() {
                MULTI_DB.store(true, Ordering::SeqCst);
            }
        }
    }

    // bm25_search(table, query, id_column := '', text_column := 'body',
    //             language := 'english', top_k := 100)
    // table and query are the only required (positional) parameters; the rest
    // are named parameters with defaults, so `bm25_search('docs','cat')` works
    // out of the box. (`top_k` rather than `limit` — `limit` is a reserved SQL
    // keyword and cannot be used as a named-argument name.)
    con.register_table(
        TableFunctionBuilder::new("bm25_search")
            .param(TypeId::Varchar)
            .param(TypeId::Varchar)
            .named_param("id_column", TypeId::Varchar)
            .named_param("text_column", TypeId::Varchar)
            .named_param("language", TypeId::Varchar)
            .named_param("top_k", TypeId::BigInt)
            .bind(bm25_bind)
            .init(bm25_init)
            .scan(bm25_scan),
    )?;

    // tantivy_create_index(name, table, [id_column := ''], [text_column := 'body'],
    //                      [language := 'english'], [path := ''])
    //   → name VARCHAR, path VARCHAR, docs BIGINT, language VARCHAR
    // Builds the persistent on-disk index and registers it (see the section
    // note above the callbacks).
    con.register_table(
        TableFunctionBuilder::new("tantivy_create_index")
            .param(TypeId::Varchar)
            .param(TypeId::Varchar)
            .named_param("id_column", TypeId::Varchar)
            .named_param("text_column", TypeId::Varchar)
            .named_param("language", TypeId::Varchar)
            .named_param("path", TypeId::Varchar)
            .bind(create_bind)
            .init(create_init)
            .scan(create_scan),
    )?;

    // tantivy_search_index(name, query, [top_k := 10])
    //   → doc_id BIGINT, score DOUBLE  (same tie-break as bm25_search)
    con.register_table(
        TableFunctionBuilder::new("tantivy_search_index")
            .param(TypeId::Varchar)
            .param(TypeId::Varchar)
            .named_param("top_k", TypeId::BigInt)
            .bind(psearch_bind)
            .init(psearch_init)
            .scan(psearch_scan),
    )?;

    // tantivy_list() → name VARCHAR, path VARCHAR, docs BIGINT, language VARCHAR
    // (snapshot of the process-global registry of persistent indexes)
    con.register_table(
        TableFunctionBuilder::new("tantivy_list")
            .bind(list_bind)
            .init(list_init)
            .scan(list_scan),
    )?;

    // tantivy_save_index(name, [path := '']) → name VARCHAR, path VARCHAR, docs BIGINT
    // (copies the index directory; without `path` reports where it lives)
    con.register_table(
        TableFunctionBuilder::new("tantivy_save_index")
            .param(TypeId::Varchar)
            .named_param("path", TypeId::Varchar)
            .bind(save_bind)
            .init(save_init)
            .scan(save_scan),
    )?;

    // tantivy_load_index(name, path) → name VARCHAR, path VARCHAR, docs BIGINT, language VARCHAR
    con.register_table(
        TableFunctionBuilder::new("tantivy_load_index")
            .param(TypeId::Varchar)
            .param(TypeId::Varchar)
            .bind(load_bind)
            .init(load_init)
            .scan(load_scan),
    )?;

    con.register_table(
        TableFunctionBuilder::new("supported_languages")
            .bind(langs_bind)
            .init(langs_init)
            .scan(langs_scan),
    )?;

    // tantivy_terms(table, text_column := 'body') → term VARCHAR, df BIGINT
    con.register_table(
        TableFunctionBuilder::new("tantivy_terms")
            .param(TypeId::Varchar)
            .named_param("text_column", TypeId::Varchar)
            .bind(terms_bind)
            .init(terms_init)
            .scan(terms_scan),
    )?;

    con.register_scalar_set(
        ScalarFunctionSetBuilder::new("tokenize")
            .overload(
                ScalarOverloadBuilder::new()
                    .param(TypeId::Varchar)
                    .returns_logical(LogicalType::list(TypeId::Varchar))
                    .function(tokenize_default_fn),
            )
            .overload(
                ScalarOverloadBuilder::new()
                    .param(TypeId::Varchar)
                    .param(TypeId::Varchar)
                    .returns_logical(LogicalType::list(TypeId::Varchar))
                    .function(tokenize_lang_fn),
            ),
    )?;

    con.register_scalar(
        ScalarFunctionBuilder::new("stem")
            .param(TypeId::Varchar)
            .param(TypeId::Varchar)
            .returns(TypeId::Varchar)
            .function(stem_fn),
    )?;

    con.register_scalar_set(
        ScalarFunctionSetBuilder::new("bm25_score")
            .overload(
                ScalarOverloadBuilder::new()
                    .param(TypeId::Varchar)
                    .param(TypeId::Varchar)
                    .returns(TypeId::Double)
                    .function(bm25_score_2_fn),
            )
            .overload(
                ScalarOverloadBuilder::new()
                    .param(TypeId::Varchar)
                    .param(TypeId::Varchar)
                    .param(TypeId::Varchar)
                    .returns(TypeId::Double)
                    .function(bm25_score_3_fn),
            ),
    )?;

    con.register_scalar(
        ScalarFunctionBuilder::new("tantivy_version")
            .returns(TypeId::Varchar)
            .function(tantivy_version_fn),
    )?;

    Ok(())
}

quack_rs::entry_point_v2!(tantivy_init_c_api, |con| register_all(con));
