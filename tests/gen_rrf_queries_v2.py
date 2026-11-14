#!/usr/bin/env python3
"""Expand semantic queries: 10 lexical + 30 paraphrases (3 per topic), same corpus."""
import json

docs = json.load(open('/tmp/rrf_bench_docs.json'))

topics = [
    ("相干时间显著提升", ["退相干难题仍未攻克", "噪声环境稳定性待改善", "纠错开销居高不下"]),
    ("海量算力支撑",     ["显卡集群开销节节攀升", "训练基础设施投资巨大", "芯片供应短缺推高成本"]),
    ("新材料通过验证",   ["极端工况耐受考验合格", "火箭部件疲劳测试达标", "深空环境器材可靠性确认"]),
    ("能量密度再创新高", ["续航焦虑缓解充电提速", "快充技术与安全性兼顾", "储能单元体积重量优化"]),
    ("疗法进入临床阶段", ["伦理审查通过疗效确认", "患者招募启动试验开展", "监管批准治疗落地"]),
    ("首次实现净点火",   ["等离子体约束取得突破", "点火能量增益超阈值", "托卡马克运行参数刷新"]),
    ("规控算法完成路测", ["无人车队闭环交付运营", "城市道路测试全部通过", "冗余系统确保接管安全"]),
    ("跨链协议正式上线", ["去中心化撮合安全审计", "链间资产转移通道开启", "共识互认机制生效"]),
    ("密钥分发网络覆盖", ["绝对安全通信干线贯通", "加密链路部署到县市", "防窃听传输设施建成"]),
    ("神经解码精度翻倍", ["侵入式采集带宽扩容", "信号噪声比大幅改善", "意念控制延迟降低"]),
]

queries = []
for ti, (lex, sems) in enumerate(topics):
    t_docs = [d for d in docs if d["topic"] == ti]
    qv = [sum(d["vec"][k] for d in t_docs) / 5 for k in range(64)]
    queries.append({"type": "lexical", "text": lex, "topic": ti, "vec": qv})
    for sem in sems:
        queries.append({"type": "semantic", "text": sem, "topic": ti, "vec": qv})

json.dump(queries, open('/tmp/rrf_bench_queries.json', 'w'), ensure_ascii=False)
print(f"queries={len(queries)} (10 lexical + {len(queries)-10} semantic)")
