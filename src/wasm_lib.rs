// Wasm target entry: the extension-ci-tools rust.Makefile builds wasm
// platforms via `cargo build --target wasm32-unknown-emscripten --example
// tantivy`, and emscripten side modules need a `staticlib` crate-type —
// there is no way to select crate-type per target in Cargo, so this file
// remaps the lib crate by path. Mirrors duckdb-html-readability's
// src/wasm_lib.rs pattern (which uses `mod lib;` because its source lives in
// src/lib/; here the crate root is src/lib.rs itself).
//
// Native builds never touch this file.
#[path = "lib.rs"]
mod tantivy_ext;
