.timer on
LOAD '/mnt/d/wsl2/duckdb-tantivy/build/release/extension/tantivy/tantivy.duckdb_extension';

CREATE OR REPLACE TABLE articles(doc_id BIGINT, title VARCHAR, abstract VARCHAR, body VARCHAR);
INSERT INTO articles VALUES
 (1, 'Quantum Computing Breakthrough', 'Physics news', 'Researchers demonstrated a new qubit design that reduces error rates by an order of magnitude.'),
 (2, 'Classical Database Tuning', 'DBA notes', 'Quantum vacuum fluctuations are irrelevant here; we tuned the buffer pool and WAL instead.'),
 (3, 'Cooking Quantum Soup', 'Kitchen diary', 'A hearty soup with potatoes; the word quantum appears once for fun.');

-- V3: body + two extra fields in ONE index
SELECT * FROM tantivy_create_index('mf', 'articles',
    id_column := 'doc_id', text_column := 'body',
    language := 'english', extra_fields := 'title, abstract');

-- 1) term that ONLY exists in title: must be found (title indexed)
SELECT '-- title-only term --' AS m;
SELECT * FROM tantivy_search_index('mf', 'breakthrough', top_k := 5);
-- 2) term in abstract only
SELECT '-- abstract-only term --' AS m;
SELECT * FROM tantivy_search_index('mf', 'physics', top_k := 5);
-- 3) term in body (V2 behavior preserved)
SELECT '-- body term --' AS m;
SELECT * FROM tantivy_search_index('mf', 'qubit', top_k := 5);
-- 4) boost title x10: doc ordering for a term present in both body and title
SELECT '-- unboosted quantum --' AS m;
SELECT * FROM tantivy_search_index('mf', 'quantum', top_k := 5);
SELECT '-- boosted title:10 --' AS m;
SELECT * FROM tantivy_search_index('mf', 'quantum', top_k := 5, boosts := 'title:10');
-- 5) V2 backward compat: old single-field index still works
SELECT * FROM tantivy_create_index('v2c', 'articles', id_column := 'doc_id', text_column := 'body', language := 'english');
SELECT * FROM tantivy_search_index('v2c', 'qubit', top_k := 5);
SELECT '-- old index (created pre-V3) still searchable: snip_test --' AS m;
SELECT * FROM tantivy_search_index('snip_test', 'cat', top_k := 3);
-- 6) snippet regression on V3 index
SELECT tantivy_snippet('mf', 'quantum', 'Quantum soup was simmering nicely') AS snip;
