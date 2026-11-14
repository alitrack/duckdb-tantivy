#!/usr/bin/env python3
"""RRF benchmark runner: FTS-only vs vec-only vs RRF hybrid on 90,050 docs.

Ground truth: per query, the 5 planted docs of its topic (ids 90001+ti*5 .. +4).
Metrics: recall@10, MRR@10.
"""
import json, subprocess, time, re

queries = json.load(open('/tmp/rrf_bench_queries.json'))
FTS = "/mnt/d/wsl2/duckdb-tantivy/build/release/extension/tantivy/tantivy.duckdb_extension"
TV = "/mnt/d/wsl2/duckdb_turbovec/build/release/extension/turbovec/turbovec.duckdb_extension"

def run(sql):
    full = (f"LOAD '{FTS}';\nLOAD '{TV}';\n.bail on\n"
            "SELECT * FROM tantivy_load_index('bench_fts', path := '/tmp/.tantivy_index/bench_fts');\n"
            f"{sql}")
    r = subprocess.run(['duckdb', '-unsigned', '/tmp/rrf_bench.db', '-noheader', '-list'],
                       input=full, capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-400:])
    return [ln for ln in r.stdout.split('\n') if ln.strip()]

res = {'fts': [], 'vec': [], 'rrf': []}
lat = {'fts': 0.0, 'vec': 0.0, 'rrf': 0.0}
for q in queries:
    truth = set(range(90001 + q['topic'] * 5, 90006 + q['topic'] * 5))
    qv = json.dumps(q['vec'])

    t0 = time.time()
    fts_rows = run(f"SELECT doc_id FROM tantivy_search_index('bench_fts', '{q['text']}', top_k := 10);")
    lat['fts'] += time.time() - t0

    t0 = time.time()
    vec_rows = run(f"SELECT idx FROM turboquant_search_ivf('/tmp/rrf_bench_ivf/', '{qv}', 10, 32, 1);")
    lat['vec'] += time.time() - t0

    t0 = time.time()
    rrf_rows = run(f"SELECT doc_id FROM tantivy_rrf_hybrid('bench_fts', '{q['text']}', '/tmp/rrf_bench_ivf/', '{qv}', top_k := 10, vec_fetch := 30);")
    lat['rrf'] += time.time() - t0

    for mode, rows in [('fts', fts_rows), ('vec', vec_rows), ('rrf', rrf_rows)]:
        ids = []
        for x in rows:
            for tok in re.split(r'[|,]', x.strip()):
                if tok.strip().isdigit():
                    ids.append(int(tok))
                    break
        hits = sum(1 for i in ids if i in truth)
        rr = next((1.0 / (r + 1) for r, i in enumerate(ids) if i in truth), 0.0)
        res[mode].append((hits / 5.0, rr))

n = len(queries)
for mode in ['fts', 'vec', 'rrf']:
    rec = sum(r for r, _ in res[mode]) / n
    mrr = sum(m for _, m in res[mode]) / n
    print(f"{mode:>4}: recall@10={rec:.3f}  MRR@10={mrr:.3f}  avg_lat={lat[mode]/n*1000:.1f}ms")

# split by query type
for qt in ['lexical', 'semantic']:
    idxs = [i for i, q in enumerate(queries) if q['type'] == qt]
    line = f"  {qt:>8}: "
    for mode in ['fts', 'vec', 'rrf']:
        rec = sum(res[mode][i][0] for i in idxs) / len(idxs)
        line += f"{mode}={rec:.2f}  "
    print(line)
