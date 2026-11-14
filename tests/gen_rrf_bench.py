#!/usr/bin/env python3
"""RRF benchmark v2 — ids aligned by construction.

id = row position (0..90049). Planted docs at 90000+ti*5 .. +4 (last 50 rows).
- lexical query: 3+ content words COPIED from the planted text -> FTS must hit
- semantic query: paraphrase with ZERO shared content words -> FTS must miss,
  vector side rescues. This is the hybrid value proposition.
"""
import json, random

random.seed(20261114)
DIM = 64
FILLER = json.load(open('/tmp/zh_corpus.json'))

topics = [
    # (planted text, lexical query, semantic query)
    ("量子比特相干时间显著提升",       "相干时间显著提升",     "退相干难题仍未攻克"),
    ("大模型训练需要海量算力支撑",     "海量算力支撑",         "显卡集群开销节节攀升"),
    ("航天发动机新材料通过验证",       "新材料通过验证",       "极端工况耐受考验合格"),
    ("动力电池能量密度再创新高",       "能量密度再创新高",     "续航焦虑缓解充电提速"),
    ("基因编辑疗法进入临床阶段",       "疗法进入临床阶段",     "伦理审查通过疗效确认"),
    ("可控核聚变首次实现净点火",       "首次实现净点火",       "等离子体约束取得突破"),
    ("自动驾驶规控算法完成路测",       "规控算法完成路测",     "无人车队闭环交付运营"),
    ("区块链跨链协议正式上线",         "跨链协议正式上线",     "去中心化撮合安全审计"),
    ("量子密钥分发网络覆盖全省",       "密钥分发网络覆盖",     "绝对安全通信干线贯通"),
    ("脑机接口神经解码精度翻倍",       "神经解码精度翻倍",     "侵入式采集带宽扩容"),
]

docs = []
# fillers first: ids 0..89999 (their original texts, random vectors)
for i in range(90000):
    docs.append({"id": i, "text": FILLER[i]["text"],
                 "vec": [random.gauss(0, 1) for _ in range(DIM)], "topic": -1})
# planted last: ids 90000+ti*5+j
for ti, (pt, _, _) in enumerate(topics):
    centroid = [random.gauss(0, 1) for _ in range(DIM)]
    for j in range(5):
        docs.append({"id": 90000 + ti * 5 + j, "text": pt,
                     "vec": [c + random.gauss(0, 0.05) for c in centroid], "topic": ti})

json.dump(docs, open('/tmp/rrf_bench_docs.json', 'w'), ensure_ascii=False)

queries = []
for ti, (pt, lex, sem) in enumerate(topics):
    t_docs = [d for d in docs if d["topic"] == ti]
    qv = [sum(d["vec"][k] for d in t_docs) / 5 for k in range(DIM)]
    queries.append({"type": "lexical", "text": lex, "topic": ti, "vec": qv})
    queries.append({"type": "semantic", "text": sem, "topic": ti, "vec": qv})
json.dump(queries, open('/tmp/rrf_bench_queries.json', 'w'), ensure_ascii=False)
print(f"docs={len(docs)} planted ids=90000..90049 dim={DIM} queries={len(queries)}")
