//! Diagnose: does an OwnedConnection opened at extension-load time (from the
//! load-time database) see tables created later on the main connection, inside
//! quack-rs's InMemoryDb (bundled duckdb 1.1.0)?
//!
//! Mirrors the extension's SESSION pattern:
//!   - main conn: open_unsigned
//!   - LOAD extension (registration callback opens OwnedConnection from the
//!     load-time database)
//!   - main conn: CREATE TABLE
//!   - then: does `SELECT * FROM t` on the main conn work? Does the extension's
//!     bm25_search (which uses the session OwnedConnection internally) see t?

use quack_rs::testing::InMemoryDb;

fn main() {
    let ext = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/release/tantivy.duckdb_extension");
    println!("ext = {}", ext.display());
    assert!(ext.exists(), "build first");

    let db = InMemoryDb::open_unsigned().expect("open db");
    db.execute_batch(&format!("LOAD '{}'", ext.display()))
        .expect("load extension");
    println!("[1] extension loaded OK");

    // sanity: does the main connection see its own tables?
    db.execute_batch("CREATE TABLE t(id BIGINT, body VARCHAR); INSERT INTO t VALUES (0,'cat sat'),(1,'dog barked')")
        .expect("create table on main conn");
    let n: i64 = db.query_one("SELECT count(*) FROM t").unwrap();
    println!("[2] main conn sees t: {n} rows");

    // 3-arg call works positionally (6 required)? try full 6-arg form:
    match db.query_one::<i64>(
        "SELECT count(*) FROM bm25_search('t','cat','id','body','english',100)",
    ) {
        Ok(c) => println!("[3] bm25_search full-6-arg on bundled 1.1.0: {c} rows"),
        Err(e) => println!("[3] bm25_search full-6-arg FAILED: {e}"),
    }

    // now the cross-connection probe, directly:
    // open a SECOND connection on the same in-memory db and check visibility
    use duckdb::Connection;
    let db2 = InMemoryDb::open_unsigned().expect("second db (different in-mem instance!)");
    // NOTE: open_in_memory creates a NEW database per call — the extension's
    // OwnedConnection comes from the FIRST database (the one that loaded the ext).
    // To probe the session connection we must ask the extension itself, since
    // we can't reach its static. Instead: create the table BEFORE load and see
    // if it's visible; then create AFTER load.
    println!("[4] (note: second InMemoryDb is a separate in-memory instance)");
    let _: i64 = db2.query_one("SELECT 1").unwrap();

    // key experiment: table created BEFORE extension load
    let db3 = InMemoryDb::open_unsigned().expect("db3");
    db3.execute_batch("CREATE TABLE before_tbl(v INTEGER)").expect("create before load");
    db3.execute_batch(&format!("LOAD '{}'", ext.display())).expect("load ext db3");
    match db3.query_one::<i64>(
        "SELECT count(*) FROM bm25_search('before_tbl','x','v','v','english',10)",
    ) {
        Ok(c) => println!("[5] table-created-BEFORE-load, text col v: {c} rows (ok)"),
        Err(e) => println!("[5] table-created-BEFORE-load FAILED: {e}"),
    }
}
