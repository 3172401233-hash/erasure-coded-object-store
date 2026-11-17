# erasure-coded-object-store

Python 纠删码对象存储内核：GF(256) Reed-Solomon 编码、两阶段提交、
版本化清单（manifest）、按版本读取与修复计划。纯标准库实现，无第三方依赖。

## 布局

- `ecstore/gf256.py` — GF(2^8) 运算（0x11D 本原多项式）、矩阵求逆
- `ecstore/codec.py` — 系统化 Reed-Solomon 编解码（Vandermonde 归一化，前 k 行为单位阵）
- `ecstore/manifest.py` — 版本清单：对象版本、分片索引、长度、SHA-256 摘要
- `ecstore/node.py` — 存储节点：pending/committed 两阶段可见性 + 故障注入
- `ecstore/cluster.py` — 协调器：put/get/repair_plan/repair
- `tests/test_ecstore.py` — 故障注入单元测试

## 分布范围

- 对象切分为 `k` 个数据分片，编码出 `m` 个校验分片，共 `k + m` 个分片，
  一一放置到 `k + m` 个不同节点上（集群节点数必须 ≥ k + m）。
- 任意 `k` 个分片即可重建对象，即最多容忍 `m` 个分片丢失/损坏/节点超时。
- 每个分片在清单中记录 `(index, node, length, sha256)`；清单另记录对象原始
  长度与整体 SHA-256，用于截掉补齐的零字节并做端到端校验。
- 每次 `put` 生成单调递增的版本号；同一对象的多个版本清单并存，读取时
  自新向旧回退。

## 写入协议（两阶段提交）

1. 编码出 `k + m` 个分片，逐节点 `stage` 到 pending 区（对读不可见）。
2. 成功 stage 的分片数 < k 时整体 `abort`，抛出 `WriteError`，不产生任何可见状态。
3. 先发布清单（提交点），再逐节点 `commit`：节点把该 `(object, version)`
   的全部 pending 分片原子翻转为 committed。**所有分片只在同一提交点之后可见**；
   错过 commit 的节点（超时/宕机）分片永远停留在 pending，读者不可见。

## 读取协议

1. 取该对象最新已提交版本的清单，**只使用该版本**的分片——跨版本绝不混读：
   节点返回的 `(version, index)` 与清单记录不一致时直接丢弃。
2. 逐个分片校验：长度与 SHA-256 必须匹配清单记录；重复分片按 index 去重，
   第一个摘要有效的副本获胜；超时/缺失/损坏的分片一律跳过。
3. 同一版本集齐 `k` 个有效分片后，用对应矩阵行求逆解码，按清单长度截断，
   再校验对象整体 SHA-256；通过才返回，保证与原始对象逐字节一致。
4. 有效分片不足 `k` 时该版本判为不可恢复，回退到更旧的已提交版本；
   所有版本都不可恢复则抛出 `UnrecoverableError`。

## 修复

- `repair_plan(object_id)`：对最新版本逐分片分类为 healthy / missing / corrupt，
  返回 `RepairPlan`；`healthy >= k` 时 `feasible=True`。
- `repair(object_id)`：先用读取协议恢复对象并校验整体摘要，再重编码出缺失/
  损坏的分片，经 stage + commit 写回原节点。健康分片不足 `k`（损坏超过 `m`）
  或目标节点不可达时抛出 `RepairError`。

## 测试

```
python -m unittest
```

覆盖：节点超时（stage/fetch/commit）、部分提交、重复分片、跨版本混读防护、
损坏 ≤ m 恢复、损坏 > m 不可恢复、修复计划与修复成功/失败路径，以及多种
长度（含 0、非 k 整数倍、随机大对象）恢复结果与原始对象逐字节一致。
