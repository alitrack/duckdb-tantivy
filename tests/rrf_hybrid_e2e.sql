
.timer on
LOAD '/mnt/d/wsl2/duckdb-tantivy/build/release/extension/tantivy/tantivy.duckdb_extension';
LOAD '/mnt/d/wsl2/duckdb_turbovec/build/release/extension/turbovec/turbovec.duckdb_extension';
CREATE OR REPLACE TABLE rrf_docs AS
SELECT row_number() OVER () AS doc_id, t, vec FROM (VALUES
  ('量子计算量子计算量子计算', [0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]),
  ('量子比特相干时间很长',     [0.1,0.12,0.1,0.1,0.1,0.1,0.1,0.12]),
  ('量子纠缠实验进展',         [0.12,0.1,0.1,0.1,0.1,0.1,0.12,0.1]),
  ('完全无关的散文一篇',       [0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9]),
  ('天气预报说明天有雨',       [0.85,0.9,0.9,0.85,0.9,0.9,0.85,0.9]),
  ('菜谱大全糖醋排骨',         [0.9,0.95,0.95,0.9,0.95,0.9,0.95,0.9]),
  ('量子菜谱量子天气预报',     [0.15,0.15,0.15,0.15,0.15,0.15,0.15,0.15]),
  ('量子实验天气预报混合',     [0.12,0.18,0.15,0.12,0.15,0.18,0.12,0.15])
) t(t, vec);
SELECT * FROM tantivy_create_index('rrf_fts', 'rrf_docs', id_column := 'doc_id', text_column := 't', language := 'chinese');
SET VARIABLE vec_str = (SELECT '[' || string_agg(CAST(vec AS VARCHAR), ',') || ']' FROM rrf_docs);
SELECT * FROM turboquant_build_ivf(getvariable('vec_str'), 8, 4, 2, '/tmp/rrf_vec_idx/', true);
-- FTS side for query 量子
SELECT doc_id, round(score,3) FROM tantivy_search_index('rrf_fts', '量子', top_k := 8);
-- vector side: query near docs 7/8 cluster
SELECT * FROM turboquant_search_ivf('/tmp/rrf_vec_idx/', '[0.14,0.16,0.13,0.14,0.13,0.16,0.14,0.13]', 8, 2);
-- THE FUSION
SELECT * FROM tantivy_rrf_hybrid('rrf_fts', '量子', '/tmp/rrf_vec_idx/', '[0.14,0.16,0.13,0.14,0.13,0.16,0.14,0.13]', rrf_k := 60, top_k := 8, vec_fetch := 8);
