# Build / stamp / test the duckdb-tantivy DuckDB loadable extension.
#
# Usage:
#   make              # == make release: build + stamp, native platform
#   make release      # release cdylib + append the 512-byte metadata footer
#   make debug        # debug cdylib + footer (for local LOAD testing)
#   make test         # cargo test --release (compiles a bundled in-memory DuckDB)
#   make clean        # cargo clean
#   make clean_all    # clean + remove stamped .duckdb_extension artifacts
#
# Overridable variables:
#   DUCKDB_PLATFORM    force the DuckDB platform string (default: auto from host arch)
#   TARGET_TRIPLE      cross-compile triple (e.g. x86_64-apple-darwin). If set, the
#                      cdylib is built for that triple and the stamper still builds
#                      for the host so it can run. Workflows build natively per runner
#                      and leave this empty.
#   EXTENSION_VERSION  extension version stamp (default: read from Cargo.toml)
#   DUCKDB_API_VERSION C_STRUCT C-API floor (default v1.2.0)

# [lib] name in Cargo.toml
LIB_NAME           := tantivy_ext
EXTENSION_NAME     := tantivy
DUCKDB_API_VERSION ?= v1.2.0
EXTENSION_VERSION  ?= $(shell sed -n 's/^version *= *"\(.*\)".*/v\1/p' Cargo.toml | head -1)
TARGET_TRIPLE      ?=

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  LIB_SUFFIX           := dylib
  NATIVE_DUCKDB_PLATFORM := $(if $(filter arm64,$(shell uname -m)),osx_arm64,osx_amd64)
else ifeq ($(UNAME_S),Linux)
  LIB_SUFFIX           := so
  NATIVE_DUCKDB_PLATFORM := $(if $(filter aarch64,$(shell uname -m)),linux_arm64,linux_amd64)
else
  LIB_SUFFIX           := dll
  NATIVE_DUCKDB_PLATFORM := windows_amd64
endif

# Where the cdylib lands: target/<triple>/release under cross, else target/release.
ifeq ($(TARGET_TRIPLE),)
  RELEASE_DIR := target/release
  DEBUG_DIR   := target/debug
  DUCKDB_PLATFORM ?= $(NATIVE_DUCKDB_PLATFORM)
else
  RELEASE_DIR := target/$(TARGET_TRIPLE)/release
  DEBUG_DIR   := target/$(TARGET_TRIPLE)/debug
endif

LIB_REL := $(RELEASE_DIR)/lib$(LIB_NAME).$(LIB_SUFFIX)
LIB_DBG := $(DEBUG_DIR)/lib$(LIB_NAME).$(LIB_SUFFIX)
# always host-arch (the stamper runs on the build host, not the target triple)
STAMPER := target/release/append_metadata
OUT_REL := $(EXTENSION_NAME).duckdb_extension
OUT_DBG := $(EXTENSION_NAME)-debug.duckdb_extension

.PHONY: all release debug test clean clean_all

all: release

release:
	cargo build --release --bin append_metadata
	cargo build --release --lib $(if $(TARGET_TRIPLE),--target $(TARGET_TRIPLE))
	$(STAMPER) $(LIB_REL) $(OUT_REL) \
	  --abi-type C_STRUCT \
	  --extension-version $(EXTENSION_VERSION) \
	  --duckdb-version $(DUCKDB_API_VERSION) \
	  --platform $(DUCKDB_PLATFORM)

debug:
	cargo build --bin append_metadata
	cargo build --lib $(if $(TARGET_TRIPLE),--target $(TARGET_TRIPLE))
	./target/debug/append_metadata $(LIB_DBG) $(OUT_DBG) \
	  --abi-type C_STRUCT \
	  --extension-version $(EXTENSION_VERSION) \
	  --duckdb-version $(DUCKDB_API_VERSION) \
	  --platform $(DUCKDB_PLATFORM)

test:
	cargo test --release

clean:
	cargo clean

clean_all: clean
	rm -f $(OUT_REL) $(OUT_DBG) *.duckdb_extension
