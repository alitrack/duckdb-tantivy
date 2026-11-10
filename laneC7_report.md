# Lane C7 report — v0.3.0 文档同步（merge 到 main 的前置）

分支 `feat/persistent-index`（基于 c719377，已 push 的 HEAD）。只改文档与注释，
**零行为改动**；未 push。

## 改动的文件

| 文件 | 改动 |
|---|---|
| `README.md` | ① 顶部加版本说明（Current release: v0.3.0，unsigned/`-unsigned`）；② 原「Persistent indexes (v0.3)」一节重写为「**Persistent named indexes (v0.3.0)**」：函数表（签名+返回列）、建→查→save→fresh 进程 load 最小示例、中文（jieba）示例用「苹果 股票」（AND 语义下仅 doc 4 命中）、持久 vs 临时选择建议、已知限制（进程内注册表无 drop / create 的 path 接管目录 / 与 bm25_search 相同的 BIGINT id·永久表·1M 行·16MiB 限制 / 无增量更新）、性能小节（合成语料实测：建索引 ≈0.3–0.7s、查询数十 ms、跨进程 bit-identical）；③ Function surface 表与 Known limitations 的 (v0.3) 标签统一为 (v0.3.0) |
| `src/lib.rs` | 2 处 **注释**（非行为）：模块头补充 v0.3 持久索引核心（原只写 ephemeral RAM index）；`bm25_search` 段头签名从内部命名 `(…, limit)` 改为实际暴露的命名参数形 `[id_column := ''], [text_column := 'body'], [language := 'english'], [top_k := 100]` |
| `Cargo.toml` | tantivy 依赖注释由「no on-disk index」改为如实描述（RAM-directory + v0.3 起 MmapDirectory） |
| `laneC7_report.md` | 本报告 |

无 `CHANGELOG.md`（已 find 确认），故按任务第 2 条在 README 顶部版本说明处提 v0.3.0。

## 自查结论（只读核对）

**doc comment ↔ 实际签名**（grep `src/lib.rs`，逐一与 `register_all` 对照）：

- `tantivy_create_index(name, table, [id_column := ''], [text_column := 'body'], [language := 'english'], [path := ''])` → `name, path, docs, language` —— 与 bind（2 positional + 4 named）/注册一致 ✓
- `tantivy_search_index(name, query, [top_k := 10])` → `doc_id BIGINT, score DOUBLE` —— 一致（top_k 缺省 10、[1,100000] 界）✓
- `tantivy_list()` → `name, path, docs, language`（无参，快照、按名排序）✓
- `tantivy_save_index(name, [path := ''])` → `name, path, docs`（无 path 仅报告位置）✓
- `tantivy_load_index(name, path := '...')` → `name, path, docs, language` —— **path 为命名参数且必填**：bind 读 `get_named_parameter_value("path")`，missing/null/empty → "path (the index directory) is required"（bind-time error）；已注册名 → "already registered"。doc 与行为一致 ✓

**tests/integration.rs 覆盖清单**（40 个 `#[test]`，其中持久索引 15 个）：
`persistent_create_summary_and_list_row`、`persistent_search_matches_bm25_search_same_params`、
`persistent_search_chinese_matches_bm25`、`persistent_search_unknown_name_is_error`、
`persistent_create_rebuild_replaces_entry`、`persistent_create_unknown_language_is_error`、
`persistent_create_missing_table_is_error`、`persistent_create_bad_names_are_errors`、
`persistent_search_top_k_bounds`、`persistent_save_and_load_roundtrip_same_process`（含
save 无 path / 复制含 meta 侧车 / 非空目标拒绝）、`persistent_load_missing_path_is_error`、
`persistent_load_name_conflict_is_error`、`persistent_list_sorted_and_stable`、
`m9_chinese_100k_acceptance_parity`（协调者原样「苹果 股票」100k 中文对拍）、
`persistent_index_100k_rows`（100k 性能冒烟）。其余 25 个为 v0.2 基线（bm25/tokenize/
stem/score/terms/languages/version + chinese 系列）。
跨进程 harness `tests/persistent_e2e.rs`：5 个 `#[ignore]`（save/load/m9×2/perf_100k），
由 `scripts/run_persistent_e2e.sh` 驱动（共 12 组 diff）。

**顺手修的文档性小错**（均注释/文案，零行为）：
1. `Cargo.toml` tantivy 注释「no on-disk index」——v0.3 起用 `MmapDirectory`，已过时；
2. `src/lib.rs` 模块头「search core builds an ephemeral RAM index per call」——v0.3 起
   同一核心还有持久索引，已补全；
3. `src/lib.rs` `bm25_search` 段头写 `limit`——实际暴露的命名参数是 `top_k`
   （`limit` 是 SQL 保留字不能做命名参数），已改为与其他段头一致的真实签名形；
4. README 中 (v0.3) → (v0.3.0) 标签统一（3 处表 + 1 处 limitations）。
其余既有注释核对无误（`search.rs` 模块 doc 已有 v0.3 持久小节；`supported_languages`
19 行注释与测试一致）。未动任何用户可见错误文案（如 "v0.1 supports a single database
instance"）——那是行为的一部分。

**README 新章节位置说明**：任务要求「放在 bm25_search 之后」；现章节位于
`Example`（bm25_search 用法）与 `Chinese tokenization (jieba)` 之后、`Hard limits`
之前——即 bm25 相关内容整体之后，且中文示例放在 jieba 说明之后阅读顺序更顺
（laneC 的 M9 验收即基于此结构），故保持原位，仅重命名+重排。

## 构建与验证（全部 --offline --locked）

- `cargo build --release --offline --locked` —— green（27.8s；3 个 warning 均为
  改动前已存在：thread_local doc comment、`BadDocsJson`/`parse_docs` 未使用）。
- 重新 stamp：`build/release/tantivy.duckdb_extension` → `target/release/`
  （extension_version=v0.3.0, ABI v1.2.0, linux_amd64）。
- `cargo test --release --offline --locked`：**lib 单元 37/37、integration 40/40、
  persistent_e2e 5 ignored**，0 失败（含 100k 中文 M9 对拍）。
- 性能自测（本机 WSL，`persistent_index_100k_rows`）：100k 建索引 **303ms**、
  两次查询（top_k 100/1000）**46ms** —— 与协调者 0.7s 一致地落在 0.3–0.7s 区间，
  README 性能小节按此口径表述并注明「合成语料实测、非基准」。

## 红线确认

- 未 push；commit 为 `docs:` 前缀，仅 4 个文档/注释文件。
- 函数行为零改动：lib.rs/Cargo.toml 的改动全部是 `//`/`#` 注释行；
  重编译后全量测试（含 M9 对拍与 100k）绿。
