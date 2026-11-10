#!/usr/bin/env bash
# Persistent-index (v0.3) e2e: rebuild, full test suite, then the CROSS-PROCESS
# save→load bit-identity check (two FRESH DuckDB processes) and the 100k-row
# performance run in a third fresh process.
#
# Usage:  ./scripts/run_persistent_e2e.sh
#         Exit 0 = everything passed, including bit-identical search results
#         across fresh processes; non-zero = something failed.
#
# Self-contained: a coordinator taking over an interrupted session can run
# this script directly to rebuild the extension and harvest test evidence.
# All cargo invocations are --offline --locked (no network, no lock
# re-resolution) and bounded by timeout.

set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== [1/6] cargo build --release --offline ==="
timeout 1800 cargo build --release --offline --locked

echo "=== [2/6] stamp .duckdb_extension metadata (same step as 'make release') ==="
PYTHON_BIN=./configure/venv/bin/python3
[ -x "$PYTHON_BIN" ] || PYTHON_BIN=python3
mkdir -p build/release
timeout 120 "$PYTHON_BIN" extension-ci-tools/scripts/append_extension_metadata.py \
  -l target/release/libtantivy.so \
  -o build/release/tantivy.duckdb_extension \
  -n tantivy -dv v1.2.0 \
  -evf configure/extension_version.txt -pf configure/platform.txt
cp build/release/tantivy.duckdb_extension target/release/tantivy.duckdb_extension
echo "stamped: target/release/tantivy.duckdb_extension"

echo "=== [3/6] cargo test --release --offline (unit + SQL e2e incl. persistent + M9) ==="
timeout 1800 cargo test --release --offline --locked

OUT="$(mktemp -d /tmp/duckdb_tantivy_e2e.XXXXXX)"
trap 'rm -rf "$OUT"' EXIT

echo "=== [4/6] cross-process save→load bit-identity (FRESH processes) ==="
E2E_OUT="$OUT" timeout 900 cargo test --release --offline --locked \
  --test persistent_e2e -- --ignored --exact fresh_process_save
E2E_OUT="$OUT" timeout 900 cargo test --release --offline --locked \
  --test persistent_e2e -- --ignored --exact fresh_process_load

echo "--- diff: persistent(after create) vs persistent(after load) — must be bit-identical ---"
diff -u "$OUT/search_create.csv" "$OUT/search_load.csv"
diff -u "$OUT/search2_create.csv" "$OUT/search2_load.csv"
diff -u "$OUT/search_zh_create.csv" "$OUT/search_zh_load.csv"

echo "--- diff: ephemeral bm25_search vs persistent tantivy_search_index (same params) ---"
diff -u "$OUT/search_ephemeral.csv" "$OUT/search_create.csv"
diff -u "$OUT/search2_ephemeral.csv" "$OUT/search2_create.csv"
diff -u "$OUT/search_zh_ephemeral.csv" "$OUT/search_zh_create.csv"
echo "all 6 diffs empty: bit-identical across fresh processes, ephemeral == persistent"

echo "=== [5/6] M9 acceptance: 100k-row CHINESE cross-process (coordinator repro) ==="
# Process A: build the coordinator's exact corpus, create + save the index,
# dump persistent AND ephemeral searches (full precision + rounded view).
# Process B (FRESH): load the saved copy, dump the same searches.
E2E_OUT="$OUT" timeout 900 cargo test --release --offline --locked \
  --test persistent_e2e -- --ignored --exact fresh_process_m9_chinese_100k_save
E2E_OUT="$OUT" timeout 900 cargo test --release --offline --locked \
  --test persistent_e2e -- --ignored --exact fresh_process_m9_chinese_100k_load

echo "--- diff: M9 persistent(after create) vs persistent(after fresh load) — must be bit-identical ---"
diff -u "$OUT/m9_p_create.csv" "$OUT/m9_p_load.csv"
diff -u "$OUT/m9_r_create.csv" "$OUT/m9_r_load.csv"
diff -u "$OUT/m9_m_create.csv" "$OUT/m9_m_load.csv"

echo "--- diff: M9 ephemeral bm25_search vs persistent tantivy_search_index (same params) ---"
diff -u "$OUT/m9_p_ephemeral.csv" "$OUT/m9_p_create.csv"
diff -u "$OUT/m9_r_ephemeral.csv" "$OUT/m9_r_create.csv"
diff -u "$OUT/m9_m_ephemeral.csv" "$OUT/m9_m_create.csv"
echo "all 6 M9 diffs empty: bit-identical across fresh processes, ephemeral == persistent"

echo "--- coordinator's literal acceptance view: SELECT doc_id, round(score,3) ... '苹果 股票' top_k 3 ---"
echo "[after fresh-process load]"; cat "$OUT/m9_p_load_rounded.csv"
echo "[after create, same process]"; cat "$OUT/m9_p_create_rounded.csv"
echo "[full precision, after fresh-process load]"; cat "$OUT/m9_p_load.csv"
echo "[discriminative rare-term '9699', after fresh-process load]"; cat "$OUT/m9_r_load.csv"

echo "=== [6/6] 100k-row performance (fresh process) ==="
E2E_OUT="$OUT" timeout 900 cargo test --release --offline --locked \
  --test persistent_e2e -- --ignored --exact fresh_process_perf_100k
cat "$OUT/perf.txt"

echo
echo "ALL PERSISTENT E2E PASS"
