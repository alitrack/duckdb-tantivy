#!/usr/bin/env python3
"""RRF benchmark v2 runner — single duckdb session, server-side loop."""
import json

queries = json.load(open('/tmp/rrf_bench_queries.json'))
FTS = "/mnt/d/wsl2/duckdb-tantivy/build/release/extension/tantivy/tantivy.duckdb_extension"
TV = "/mnt/d/wsl2/duckdb_turbovec/build/release/extension/turbovec/turbovec.duckdb_extension"

sql = [f"LOAD '{FTS}';", f"LOAD '{TV}';", ".bail on",
       "SELECT * FROM tantivy_load_index('bench_fts', path := '/tmp/.tantivy_index/bench_fts');"]
for i, q in enumerate(queries):
    qv = json.dumps(q['vec'])
    truth = set(range(90000 + q['topic'] * 5, 90005 + q['topic'] * 5))
    sql.append(f"SELECT 'FTS{i}' AS tag, list(doc_id) FROM tantivy_search_index('bench_fts', '{q['text']}', top_k := 10);")
    sql.append(f"SELECT 'VEC{i}' AS tag, list(idx) FROM turboquant_search_ivf('/tmp/rrf_bench_ivf/', '{qv}', 10, 32, 1);")
    sql.append(f"SELECT 'RRF{i}' AS tag, list(doc_id) FROM tantivy_rrf_hybrid('bench_fts', '{q['text']}', '/tmp/rrf_bench_ivf/', '{qv}', top_k := 10, vec_fetch := 30);")

open('/tmp/rrf_bench_exec.sql', 'w').write('\n'.join(sql) + '\n')
print("sql written,", len(sql), "statements")
