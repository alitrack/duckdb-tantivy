// DuckDB loadable extension: BM25 full-text search, tokenization, and Snowball
// stemming, powered by tantivy.
//
// This file is a thin quack-rs binding layer over `search.rs` (a pure Rust
// search core with no DuckDB dependency). The search core builds an ephemeral
// RAM index per call; this layer plumbs SQL inputs into it and returns
// vectors/hits back to DuckDB.

use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

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
// Table function: supported_languages() → lang VARCHAR (18 rows)
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
            unsafe { writer.write_varchar(i, langs[state.next + i]) };
        }
        state.next += batch;
        unsafe { chunk.set_size(batch) }
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

    con.register_table(
        TableFunctionBuilder::new("supported_languages")
            .bind(langs_bind)
            .init(langs_init)
            .scan(langs_scan),
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
