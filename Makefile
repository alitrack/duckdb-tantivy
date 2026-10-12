.PHONY: clean clean_all cargo_test

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=tantivy
EXT_CONFIG=$(PROJ_DIR)extension_config.cmake

# Set to 1 to enable Unstable API
USE_UNSTABLE_C_API=0

# The stock ci-tools test target runs duckdb_sqllogictest against test/sql,
# which this repo does not use (our e2e suite is the Rust integration tests,
# run via `make test` in our own CI). Mirror duckdb_turbovec's pattern so the
# DuckDB community-extensions pipeline's `make test_*` is a clean no-op.
SKIP_TESTS=1

# Target DuckDB C API version (not DuckDB release version)
TARGET_DUCKDB_VERSION=v1.2.0

# The stock ci-tools test target runs duckdb_sqllogictest against test/sql
# (empty here); our real e2e suite is the Rust integration tests, so rebind
# `test` to `cargo_test` after the includes.

# Include makefiles from DuckDB
# A freshly cloned repo has a .gitmodules but no submodule checkout yet;
# fail with an actionable message instead of a cryptic include error.
ifeq ($(wildcard extension-ci-tools/makefiles/c_api_extensions/base.Makefile),)
$(error extension-ci-tools is missing. Run: git submodule update --init --recursive)
endif
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

all: configure release

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: cargo_test
test_debug: test_extension_debug
test_release: test_extension_release

# e2e: build+stamp (release), mirror the artifact to target/release/ where the
# Rust test harness (tests/integration.rs) LOADs it, then run the cargo suite
# (unit + 17 SQL-level integration tests against a bundled in-memory DuckDB).
cargo_test: release
	$(PYTHON_VENV_BIN) -c "import shutil,os; os.makedirs('target/release', exist_ok=True); shutil.copyfile('$(EXTENSION_BUILD_PATH)/release/$(EXTENSION_FILENAME)', 'target/release/$(EXTENSION_NAME).duckdb_extension')"
	cargo test --release

clean: clean_build clean_rust
clean_all: clean_configure clean
