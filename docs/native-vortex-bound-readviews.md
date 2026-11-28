# R6 Typed UUID、发布与 ReadView 的统一绑定

本轮以 #93 合入后的开发提交 `7716a403` 为基线，通过 BoundCatalog 将 typed 元数据接入 PostgreSQL／S3 broker。[BoundCatalog 实现](../scripts/vortex_bound_catalog.py) · [类型化元数据](native-vortex-postgres-typed-lifecycle.md) · [发布 broker](native-vortex-postgres-s3-backend.md) · [总设计与 roadmap](ducklake-vortex-design-roadmap.md)

**本轮验证组件身份与可见性契约。** Source／index 使用不透明 bytes fixture，未生成或查询原生 Vortex／SPFresh 文件，不能据此宣称远程 ANN、过滤或 top-k 已接通。原生 C++／Rust、SQL、provider 和远程配置限制保持不变。

本地固定 CPU **6–11**，继承现有线程环境，原生 `threads=2` 不变；事务保护与预算保持。R4 A/A、性能 A/B、长期稳态及依赖稳定负载的 R5 资格继续暂缓，耗时仅作观察。

## 一个明确的身份边界

Storage 和 typed catalog 的布局号都是 **`1`**。使用当前 DDL 初始化全新 PostgreSQL schema，并以该新建 schema 进行重启和恢复验证。早期实验 schema 保留在各自的历史验证中。#92 的单根入口用于底层原语回归，typed 发布入口根据显式 typed 身份解析业务目标。

新增四张关联表；`units` 增加不可空 UUID：

| 表 | 固定的身份与用途 |
| --- | --- |
| `bound_units` | Source／generation 绑定 table UUID、table ID、schema hash、维度、vector field ID、全部 field ID、源 snapshot、物理 row ID 和 typed 行摘要 |
| `bound_requests` | 发布请求固定 `(table_uuid, request_epoch, request_id)`，连接原 `requests`；不同 UUID／epoch 可使用相同请求名 |
| `bound_heads` | 每个 UUID 自己的当前 root／revision；退休后清空 root，保留 revision 历史 |
| `bound_readers` | 持久化查询 snapshot、字段身份、source 身份、root／revision 与物理可见行交集 |

`bound_units` 通过复合外键同时约束 typed table 的 UUID／table ID／schema／维度，以及 storage unit 的 ID／UUID。API 还从实际字段与 typed 历史重新计算身份，拒绝跨 UUID source、GC、错误 schema 和没有持久化绑定的接管；相同 bytes／SHA 不能替代业务身份。跨行身份使用外键，跨步骤变更使用 PostgreSQL 事务。[PG 约束](https://www.postgresql.org/docs/15/ddl-constraints.html)

注册、封存、发布、捕获与退休沿用 `control … FOR UPDATE` 串行化。绑定写入口要求正整数 owner token，并在同一事务中检查当前 owner。状态查询和回执对账检查 storage 布局、对象存储绑定和 typed 布局。Source／generation、head、引用和回执一起提交。实际 GC 原语在其事务内保留 owner、引用、intent 与预算校验，复合外键固定 unit 的 UUID。[PG 行锁](https://www.postgresql.org/docs/15/explicit-locking.html)

## 发布、窗口与退休

Source 身份记录源 snapshot 的 typed 行与物理地址，generation 继承该身份。请求摘要同时绑定 source unit 和已封存对象的 key／version／size／SHA。Manifest 固定 catalog、UUID／字段／schema／snapshot、请求身份、原 expected revision、store，以及 source／index 的精确对象身份。Source／index bytes 仍由调用方提供，本轮没有证明这些 bytes 编码了对应 typed 行。

发布 CAS 使用目标 UUID 自己的 revision：另一 UUID 先发布不会使本 UUID 的 prepared 请求过期。`control.revision`／`head` 记录整个 catalog 的发布次序和最后一次发布，不能当作任意表的当前 root。准备时固定 expected revision，显式 adopt 只改 owner claim；禁止改绑定、换 source 或自动 rebase。

Typed epoch 推进后，旧 prepared 发布必须显式取消并在原预算内回收。在同一 UUID 和有效 epoch 内，`prepare` 对相同输入返回已有请求，即使 source 已过期或完成 GC。新请求仍要求有效的 native 引用。发布回执作为物理 lineage 历史保留，`reconcile` 按原 UUID／epoch／ID／摘要查找。业务 mutation 回执仍按窗口推进和退休规则清除。

退休在同一 PostgreSQL 事务内结束目标 UUID 的行版本、释放 managed／epoch／业务回执、解除该 UUID 的实际 named 引用、清空 root，并把失去 named 引用的 ready generation 标为 retired。存在 allocated／deleting unit 时拒绝退休，包括取消后尚未完成 GC 的构建；没有隐藏清理。事务中途错误或 COMMIT 转发前断开时全部回滚，服务端 COMMIT 后丢 ACK 时全部提交。

封存要求 source 仍处于 allocated，且持有以该 unit 为 holder 的 build 引用。当前 owner 可用 `abandon_source` 显式放弃未封存 source。该操作解除 build 引用，关闭后续 ticket 和 seal，保留登记身份供原 GC 协议回收。退休仍须等待该 allocated unit 完成回收。

Native 历史引用和 reader pin 独立保留。活跃的普通 SQL session 在整个 catalog 范围内阻止 source 历史过期和 GC 准入。已准入的 source GC 完成前，普通 SQL capture 返回 `SOURCE_GC_ACTIVE`。回收继续使用显式 intent、STOP 和精确版本删除。

## ReadView 与同名重建

一次 capture 固定查询当时的 typed snapshot 和全部 pin。Source 的构建 snapshot 可以早于查询 snapshot，因此视图同时保存两种身份，并用**物理 row ID 交集**描述 source 在查询中的可见性。更新／删除的旧物理行消失，新 inline 行有新地址；没有按业务键把旧向量混回当前版本。

`materialize` 在 control 行锁内验证 typed 行摘要和完整 pin，固定精确对象版本。远程 GET 在该锁外执行，完成后重新持锁验证视图和 pin，再保存完整私有副本。若视图在 GET 期间关闭或解除 pin，当前物化失败。完整副本可显式解除 pin，在表退休、同名重建、原对象 GC 和 owner 接管后仍可读取。Server 重启测试保留了独立 reader 对象；不宣称 reader 进程死亡后其内存副本可恢复。

回收的 STOP body 使用 **unit 登记的 UUID**，不再使用单例 fixture 的 table seed。旧 UUID 的精确 DELETE 可跨越同名重建后完成，但只删除旧 key 的旧版本；旧 PUT 被已保留的 STOP 拒绝，不会触及新 UUID 对象。没有通过 LIST 接管未知文件或自动删除 STOP。

## 预算与确定性验收

Source／artifact／向量各 **64 MiB**，manifest **1 MiB**，物理行 **100,000**，维度 **1–4096**，静态构建至少 **65** 行；typed capture 在读取变长值前按保守上界检查 64 MiB。GC 每个显式预算最多 **16 个逻辑单元／64 MiB**，一个预算内各动作顺序执行。业务 mutation 仍为 **1–1,000 操作／4 MiB**、catalog **100,000 回执**。原生三 B、全局 worker 和 admission 不变；实验性入口每 UUID 当前一根，不代表原生多 B 已完成远程资格。

[Coordinator](../test/vane/test_vane_vortex_bound_catalog.py) 启动全新真实 PostgreSQL／MinIO，[独立 oracle](../test/vane/vortex_bound_catalog_oracle.py)直接读取 **16 张表**及全部 S3 版本／bytes，另外复用独立 typed 历史模型。行摘要由服务端 `float4send`／`float8send`、BYTEA 与单独二进制编码重新计算，不调用新入口的 schema／codec／manifest／引用／回收判定。私有副本逐值核对 IEEE、NULL、文本、BLOB、列表及对象 SHA。每次预期拒绝都要求完整 metadata／对象状态不变。

| 固定场景 | 验收条件 |
| --- | --- |
| 跨 UUID 与同名请求 | 相同 source／request 名和相同 opaque bytes 仍相互隔离；非连续 field ID、错误 table ID／storage UUID 的复合外键拒绝；另一 UUID 发布不干扰本地 CAS |
| 窗口与可见性 | 旧 epoch 发布拒绝，取消后显式 GC；65 行 source 经更新／删除／新插入后，当前查询仍 65 行，可见旧物理行恰为 63，旧视图仍 65 |
| 事务原子性 | 退休／发布各有暂存后 trigger 错误，以及 COMMIT 转发前断开／确认后丢响应；必须全旧或全新，再按原身份显式查询或重试 |
| 退休与私有视图 | Named root／引用与 typed 生命周期一起释放，reader pin 阻止 GC；完整副本解除 pin 后经历规范 GC 和同名新 UUID，逐值保持 |
| 真正晚到的对象请求 | HTTP 握手暂停旧 DELETE，期间创建并发布同名新 UUID，放行后旧版本 DELETE 为 204；取消并 GC 后才放行的旧 PUT 为 412 |
| 故障与接管 | 实际 SIGKILL PG／MinIO，重启先核对完整状态，再 fencing 旧 owner、显式 adopt 原请求并发布；原 expected revision、输入身份和原 reader 副本保持 |
| 写入与回执 | 无效 owner 在写入口被拒绝，接管后发布仍要求请求已 adopt。配错存储前缀或缺少 typed 布局时拒绝查询、对账和取消 |
| 请求重放 | 相同 typed snapshot 的不同物理 source 得到不同请求摘要。旧 source 过期和 GC 后，完全相同的 prepare 与对账均返回原结果 |
| Source GC | 验证普通 SQL 和 source GC 的两种准入顺序，以及上传中断后的显式放弃、精确封存保护和 STOP 回收 |
| 私有副本物化 | 固定 GET 等待点。独立表可以提交；关闭视图或解除 pin 后，在途物化不得登记副本 |

## 复现与证据

```bash
/path/to/isolated/python -I test/vane/test_vane_vortex_bound_catalog.py \
  --cpus 6,7,8,9,10,11 --pg-bin /path/to/postgresql-15/bin \
  --minio /path/to/pinned/minio --object-data-dir /fresh/objects \
  --output-dir /fresh/bound-proof
```

依赖继续使用[固定 requirements](../test/vane/requirements-remote-backend.txt)，本地 PostgreSQL **15.13**，MinIO **RELEASE.2025-09-07T16-13-09Z**／SHA `7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f`。本机对象 fixture 显式指定新 `/dev/shm` 目录，结束后只移除自己创建的目录；没有自动切换存储路径。[CI](../.github/workflows/VortexRemoteBackend.yml)依次运行协议模型、backend、typed 和绑定专项，保留报告与服务日志，不运行稳定负载测试。

### 当前开发基线验收

在新建 `layout=1` schema 上，绑定专项通过 **349 项检查，其中 66 项预期拒绝**，并完成 10 组故障和并发观测。PostgreSQL 和 MinIO 的实际 SIGKILL 退出码均为 `-9`。恢复在这次运行所创建的 schema 中验证。

同基线的 typed 回归通过 **175 项检查，其中 91 项预期拒绝**，完成 9 组故障观测。后端回归通过 **485 项检查，其中 35 项预期拒绝**，完成 9 项能力探测和 7 组故障观测。协议模型通过 **40 个场景和 21 个反例**，探索的 1,342 个状态及状态集合摘要与开发基线一致。各套真实服务回归均在 180 秒期限内完成。

### 原叠栈验收

2026-11-07 的原 PR 基于 #93 旧叠栈提交 `62b87b87`。历史专项通过 **224 项检查，其中 21 项预期拒绝，7 组故障观测**，PG 和 MinIO 实际 SIGKILL 退出码均为 **-9**。历史报告路径为 `/tmp/ducklake-vortex-bound-proof-v4-20261107/report.json`，SHA-256 为 **`1bb073b2fcfbab2792c11575ea44600b8c3d77e318d6661aa5fac033f002459a`**。报告记录检查名与完整状态摘要、协议帧和 HTTP 握手、最终 16 表与对象版本、执行源码 SHA、实际 CPU、线程环境和恢复结果。

原叠栈上的关联回归通过以下检查。

| 回归 | 结果 | report.json SHA-256 |
| --- | --- | --- |
| #93 typed | 121 检查／51 拒绝／9 故障，writer＋PG＋MinIO 三次 SIGKILL | `b487825941171f7186fd573b0801c2de43a4d80c3f44f8d280d950df316c0495` |
| #92 backend | 458 接口／9 能力／7 故障 | `bd4450ba67630dae46da94e7dca0e827cb484cd0717fcea8a8a947d224d4ddf1` |
| #91 模型 | 39 场景／18 不安全变体反例，905＋437 状态，状态集合摘要保持 | `67576184923a1b3b4131a4b96804f3810e69f3a355f5f43892d0e54dc78ba644` |

回归报告分别保留在 `/tmp/ducklake-vortex-bound-{typed,backend,model}-regression-20261107/`。由于本机磁盘余量不足，typed／backend／最终绑定专项的服务目录也显式放在独立 `/dev/shm` fixture，完整 JSON 和服务日志另外复制保留至上述 `/tmp` 路径；原报告未改写。

原叠栈的 Vane 基础 launcher 在 CPU 6–11 分独立非 Ray 和 Ray 进程运行，通过 **1,254 项**，因缺少可选依赖跳过 **15 项**。历史日志路径为 `/tmp/ducklake-vortex-bound-base-20261107.log`。

## 剩余边界与下一项

下一项推荐 **R6 reader session 所有权／fencing 与持久 pin 的显式回收**：先证明崩溃 reader 的 pin 何时能安全释放，旧 session 何时必须停止访问；普通 SQL 事务保护继续保留，不能仅因时间过去就撤销活跃视图。

之后还需原生 typed codec 与 source／index 编码接线、普通事务自动登记、原生远程查询、真实 AWS／TLS／IAM、HA／掉电、固定 ReadView fan-out／读位置等待／分布式 top-k。Typed 历史、协议 metadata 和 STOP 累积尚无总空间／WAL／RSS 上界。R7 SPFresh 原生在线更新仍未接入；本轮不解除原生 native DuckDB／本地 POSIX 限制。
