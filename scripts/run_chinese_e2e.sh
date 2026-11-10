#!/usr/bin/env bash
# One-shot rebuild + full test suite + Chinese-specific (jieba) assertions.
#
# Usage:  ./scripts/run_chinese_e2e.sh
#         Exit 0 = everything passed; non-zero = something failed.
#
# Self-contained: a coordinator taking over an interrupted session can run
# this script directly to rebuild the extension and harvest test evidence.
# All cargo invocations are --offline (no network) and bounded by timeout.

set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== [1/4] cargo build --release --offline ==="
timeout 1800 cargo build --release --offline

echo "=== [2/4] stamp .duckdb_extension metadata (same step as 'make release') ==="
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

echo "=== [3/4] cargo test --release --offline (full suite: unit + SQL e2e) ==="
timeout 1800 cargo test --release --offline

echo "=== [4/4] Chinese (jieba) focused assertions — evidence run ==="
timeout 900 cargo test --release --offline --lib -- --nocapture chinese
timeout 900 cargo test --release --offline --test integration -- --nocapture chinese
timeout 900 cargo test --release --offline --test integration -- --nocapture zh_alias

echo
echo "ALL CHINESE E2E PASS"
