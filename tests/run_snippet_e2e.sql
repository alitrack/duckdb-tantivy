.timer on
LOAD '/mnt/d/wsl2/duckdb-tantivy/build/release/extension/tantivy/tantivy.duckdb_extension';

CREATE OR REPLACE TABLE docs(doc_id BIGINT, body VARCHAR);
INSERT INTO docs VALUES
 (0, 'The cat sat on the mat. It was a sunny afternoon and the cat purred loudly.'),
 (1, 'A dog barked at the mailman. Dogs are loyal animals that love to play fetch.'),
 (2, 'Quantum computing uses qubits to perform calculations much faster than classical computers.');

SELECT * FROM tantivy_create_index('snip_test', 'docs', text_column := 'body', language := 'english', id_column := 'doc_id');

SELECT '-- 1) basic: stem-match cat --' AS m;
SELECT doc_id, tantivy_snippet('snip_test', 'cat', body) AS snip FROM docs WHERE doc_id = 0;
SELECT '-- 2) two terms --' AS m;
SELECT doc_id, tantivy_snippet('snip_test', 'cat mat', body) AS snip FROM docs WHERE doc_id = 0;
SELECT '-- 3) no match -> leading fragment --' AS m;
SELECT doc_id, tantivy_snippet('snip_test', 'zebra', body) AS snip FROM docs WHERE doc_id = 1;
SELECT '-- 4) RAG join pattern --' AS m;
SELECT r.doc_id, r.score, tantivy_snippet('snip_test', 'quantum computing', d.body) AS snip
FROM tantivy_search_index('snip_test', 'quantum computing', top_k := 3) r
JOIN docs d ON d.doc_id = r.doc_id
ORDER BY r.score DESC;
SELECT '-- 5) Chinese jieba --' AS m;
CREATE OR REPLACE TABLE zh(doc_id BIGINT, body VARCHAR);
INSERT INTO zh VALUES (0, '量子计算使用量子比特进行运算，速度远超经典计算机。量子比特可以同时处于多个状态。');
SELECT * FROM tantivy_create_index('zh_test', 'zh', text_column := 'body', language := 'chinese', id_column := 'doc_id');
SELECT tantivy_snippet('zh_test', '量子比特', body) AS snip FROM zh WHERE doc_id = 0;
SELECT '-- 6) unknown index error --' AS m;
SELECT tantivy_snippet('no_such_index', 'cat', 'x');
