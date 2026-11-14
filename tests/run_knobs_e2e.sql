.timer on
LOAD '/mnt/d/wsl2/duckdb-tantivy/build/release/extension/tantivy/tantivy.duckdb_extension';

CREATE OR REPLACE TABLE zh(doc_id BIGINT, body VARCHAR);
INSERT INTO zh VALUES (1,'量子计算使用量子比特进行运算'),(2,'华为的麒麟芯片很强大'),(3,'今天天气很好适合出门运动');

-- knob 1: default language via SET (no language := arg anywhere)
SET tantivy_default_language = 'chinese';
SELECT * FROM tantivy_create_index('zhidx', 'zh', id_column := 'doc_id', text_column := 'body');
-- jieba must have been used (chinese took effect): query 量子比特 hits doc 1
SELECT '-- knobl: chinese via SET --' AS m;
SELECT * FROM tantivy_search_index('zhidx', '量子比特', top_k := 5);

-- knob 2: default top_k via SET (no top_k arg)
SELECT '-- knob2: top_k via SET --' AS m;
SET tantivy_default_top_k = 1;
SELECT * FROM tantivy_search_index('zhidx', '量子');

-- explicit args still win over knobs
SELECT '-- explicit wins --' AS m;
SET tantivy_default_top_k = 1;
SELECT * FROM tantivy_search_index('zhidx', '量子', top_k := 3);

-- RESET restores default (top_k back to 10)
SELECT '-- reset --' AS m;
RESET tantivy_default_top_k;
SELECT count(*) AS n FROM tantivy_search_index('zhidx', '量子');

-- current_setting() sees them
SELECT current_setting('tantivy_default_language') AS lang;
