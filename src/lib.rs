// DuckDB loadable extension: BM25 full-text search, tokenization, and Snowball
// stemming, powered by tantivy.
//
// This file is a thin quack-rs binding layer over `search.rs` (a pure Rust
// search core with no DuckDB dependency). The search core builds an ephemeral
// RAM index per call; this layer plumbs SQL inputs into it and returns
// vectors/hits back to DuckDB.

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
// Session connection
//
// Table-function callbacks (bind/init/scan) have no `duckdb_connection` — the
// C API only hands them a client context. `bm25_search` must run SQL
// (`SELECT id, text FROM {table}`) against the user's table, so we open one
// dedicated `OwnedConnection` during registration (from the load-time
// database) and keep it. It holds a shared_ptr to the database instance, so it
// stays valid for the whole session. Shared behind a `Mutex` (OwnedConnection
// is `Send` but not `Sync`; one connection at a time is all we need — the
// extension is otherwise stateless).
// ─────────────────────────────────────────────────────────────────────────────

static SESSION: OnceLock<Mutex<OwnedConnection>> = OnceLock::new();

fn session() -> Option<MutexGuard<'static, OwnedConnection>> {
    SESSION.get().and_then(|m| m.lock().ok())
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
            Some(con) => match con.query(&probe) {
                Ok(_result) => {}
                Err(e) => {
                    bind_info.set_error(&format!("bm25_search: cannot open table '{table}': {e}"));
                    return;
                }
            },
            None => {
                bind_info.set_error("bm25_search: extension session is unavailable");
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
        let mut text_len = 0usize;
        {
            let Some(con) = session() else {
                init_info.set_error("bm25_search: extension session is unavailable");
                FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                return;
            };
            let mut result = match con.query(&sql) {
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
                            // Per-text bound (the scalar path applies
                            // search::MAX_TEXT_BYTES via check_text_len; the
                            // table path enforces it here so one 20MB row
                            // cannot be silently indexed).
                            if text.len() > search::MAX_TEXT_BYTES {
                                init_info.set_error(&format!(
                                    "bm25_search: a row in '{table}' exceeds the {b}-byte per-text limit",
                                    table = bind.table,
                                    b = search::MAX_TEXT_BYTES
                                ));
                                FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
                                return;
                            }
                            text_len += text.len();
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
                        FfiInitData::<Bm25State>::set(info, Bm25State {
                            hits: Vec::new(),
                            next: 0,
                        });
                        return;
                    }
                }
            }
            // `result` and `con` drop here: the session lock is released before
            // the (lock-free) BM25 run below.
        }

        // Memory guard (P0-2): without this, an unbounded table materializes
        // every row plus a RAM index before BM25 runs — a 20M-row table is
        // multi-gigabytes of resident memory. The scalar path enforces these
        // bounds via search::check_text_len / MAX_DOCS; the table path must
        // enforce them here.
        let text_cap = search::MAX_DOCS * 1024; // ~1KB of text per doc, worst case
        if docs.len() > search::MAX_DOCS || text_len > text_cap {
            init_info.set_error(&format!(
                "bm25_search: table '{table}' has {n} rows ({t} bytes of text); the limit is {d} rows — add a WHERE clause",
                table = bind.table,
                n = docs.len(),
                t = text_len,
                d = search::MAX_DOCS
            ));
            FfiInitData::<Bm25State>::set(info, Bm25State { hits: Vec::new(), next: 0 });
            return;
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
    // One session connection for the lifetime of the extension, so table
    // callbacks can run SQL (see the module note above). Idempotent in case
    // DuckDB re-enters registration for the same process.
    if SESSION.get().is_none() {
        let owned = con.open_connection()?;
        let _ = SESSION.set(Mutex::new(owned));
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
