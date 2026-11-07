# R6 Reader session 所有权、fencing 与持久 pin 回收

本轮基于 [Draft #94](https://github.com/AstroVela/ducklake/pull/94) 的 `5b7c01b378278ea147c9e62b6390d21bb4f5de65`，在 [UUID／发布／ReadView 绑定](native-vortex-bound-readviews.md)上增加必须显式携带的 reader session。总入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。

本地保持 CPU **6–11**、继承的线程环境、原生 `threads=2`、事务保护和预算。R4 A/A、性能 A/B、长期稳态及依赖稳定负载的 R5 资格继续暂缓，时间只作观察。本轮是实际 PG／MinIO 的组件契约验证；source／index 仍为 opaque fixture，未接入原生 Vortex／SPFresh 编码或远程 ANN。

## 明确的一次会话身份

[ReaderSession／ReaderReclaimer](../scripts/vortex_reader_sessions.py)与 [BoundCatalog](../scripts/vortex_bound_catalog.py)采用 **storage v2／全新 typed v3** 布局。没有旧 typed 布局读取、迁移、可选旧 reader 入口、自动重连、会话接管或 fallback；原生 SQL／provider 不变。

| 持久表 | 内容 |
| --- | --- |
| `reader_sessions` | Session UUID、owner UUID、advisory key、PG backend PID／start、fence、状态、原 intent／完成回执 |
| `reader_session_views` | 每个 typed ReadView 必须绑定唯一 session UUID／原 fence；与 capture 的 root、snapshot 和全部 pin 同事务提交 |
| `reader_reclaims` | 逐视图释放的原回执，包括 session／fence、实际释放 unit 集、零 payload bytes；复合外键限制所属 session／view |

Session UUID 不复用。一个 incarnation 从 fence **1** 的 active 开始，只能正常关闭，或经过显式回收进入 terminal fence **2**；没有猜测新 incarnation 或循环使用旧 ID。业务 UUID／request epoch 与 reader fence 是不同身份。

`open_session` 创建一条专用 PG 连接，持有由 catalog UUID／session UUID 派生的独占 session-level advisory lock，并持久化实际 PID／backend start。Session-level 锁随服务端会话结束释放；事务级 advisory 锁可用于不等待地探测竞争，行锁继续保护 metadata 原子变更。[PG advisory locks](https://www.postgresql.org/docs/15/explicit-locking.html#ADVISORY-LOCKS)、[锁函数](https://www.postgresql.org/docs/15/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)

普通 reader RPC 在独立 metadata 事务中取得既有 `control … FOR UPDATE`，同时核对 session 状态／fence／owner、PID／start 和实际锁所有权；`pg_locks` 的数据库、key 高低位及锁类型全部匹配。[PG 锁视图](https://www.postgresql.org/docs/15/view-pg-locks.html) 锁 key 冲突会拒绝创建，不寻找替代 key。

## 对象访问与私有副本

Capture、materialize、解除 copy pin 和关闭视图都必须携带所属 session。跨 session 的 materialize／close／unpin 在访问 S3 前拒绝；writer owner 轮换不撤销仍活跃的独立 reader。

Materialize 在**每个 GET 前后**及操作结束时重查 lifetime lock；不重连任一丢失的 PG 连接。只有完整 typed 行、全部确切对象版本和所有身份检查完成，并且操作事务成功提交后，才发布到私有副本集合。COMMIT 不确定时不发布副本、不自动解除 pin。

Reader 操作与 reclaimer 在 control 行锁上排序：回收先等待已经开始的操作结束。如果 GET 完成后 lifetime 连接丢失，旧结果不能变成可解除 pin 的副本，也不能继续 GET 下一个对象；如果操作连接也被终止，恢复的旧调用同样失败。Fenced session 的后续 RPC 不触达 S3。已经在 fence 前成功提交并交给调用方的 bytes 不被撤销；它们不会授予新的对象访问资格。

`disconnect` 只关闭自己的 lifetime 连接，不修改持久 pin。正常 `close_session` 要求所属视图全部关闭；成功提交后才断开连接。关闭的 COMMIT 不确定也会关闭自己的连接，但 metadata 只能通过原 session 身份查询／显式回收，不自动清理或重新打开旧 UUID。

## 显式、受预算的回收

| 步骤 | 原子边界 |
| --- | --- |
| `begin(session_uuid, original_fence, owner_token, budget)` | 在 control 锁下尝试同 key 的事务级独占 advisory lock；活跃 holder 返回 `READER_ACTIVE`。只有原 holder 锁已释放，才能持久化原 intent 并推进 terminal fence；此步不释放任何 view／pin |
| `view(..., reader_id, ...)` | 校验原 intent 和 session／view 所有权，原子删除该视图全部 pin、标记视图关闭、保存原回执。中途错误全部回滚 |
| `finish(...)` | 只有所属视图全部关闭且 pin 全部消失，才保存最终回执。重复 begin／view／finish 使用原身份和原回执，不重新绑定 |

维护继续使用当前 writer owner fence；旧 writer 不可回收，后续 owner 可按同一 session／intent 恢复。产品回收只尝试锁，不调用 `pg_terminate_backend` 杀死 holder。测试中终止的是专用 fixture backend，用于注入连接损失。

调用方传入既有 **1–16 个 work items／64 MiB** 的同一 `Budget`，回收器内部不创建预算、不自动循环。一个 session intent／完成合计占一个逻辑 work item，每个 view 占一个；当前 view 最多两个 unit pin。这里只操作 metadata，payload bytes 计 **0**，不读取或删除 S3 对象。它与 canonical GC 共用工作项和字节计数；预算耗尽时必须由调用方显式开启下一轮。已预留项遇到回滚／未知提交不会返还，原回执重放不重复扣除。

Fencing 不提前撤销普通 SQL 保护：尚未被显式关闭的 `kind=sql` view 仍全局阻止 source 历史释放，包括 reader 已崩溃、已 fence 或部分回收之后。完成 SQL view 的显式回收后，仍要分别过期 native 引用，再按原 GC intent／STOP／精确版本删除协议回收规范对象。

没有 TTL、心跳超时后直接删 pin、未知对象 LIST 接管、自动副本接管或第二份日志。一个没有证据证明断开的 holder 可以继续阻止回收；本轮不承诺网络故障发现的时间上界。

## 独立确定性验证

[Coordinator](../test/vane/test_vane_vortex_reader_sessions.py)复用真实 PG／MinIO fixture；[新 oracle](../test/vane/vortex_reader_sessions_oracle.py)读取新增三表及实际 PG lock facts，再联合既有独立 typed／绑定 oracle 核对 **19 张表**和全部 S3 版本／bytes。它不调用 session 授权、锁判定、fence 或回收函数；独立推导 advisory key、核对历史 owner／view 不转移、原回执不变、私有 typed 值／完整对象副本和实际释放 pin 集。每次预期拒绝保持完整持久状态不变；pin maintenance 不得改变业务 typed 历史或 canonical 对象。

| 固定边界 | 验收 |
| --- | --- |
| 活跃、暂停与崩溃 reader | 独立 reader broker 进程实际 SIGSTOP／SIGCONT，锁仍在时回收拒绝；实际 SIGKILL 后 pin／SQL 保护仍持久，只有显式 fence／逐 view 回收才解除 |
| 在途 GET | 真实 GET 已完成但结果在测试握手处暂停，终止 lifetime backend；观察 reclaimer 的真实 PG 锁等待，再放行旧结果，必须不发布副本／不继续 GET。另一场同时终止操作 backend，回收可先提交，旧调用恢复后仍失败 |
| 所有权与约束 | 跨 session 的读取／unpin／close／回收、重复 UUID、非法 fence、旧 writer、复合外键及 CHECK 拒绝；writer 轮换后活跃 reader 仍可 materialize |
| 全局预算 | 17 个 view；同一预算先登记一个 canonical GC unit、一个 session intent，再释放 14 个 view，恰好 16 项；下一项拒绝。另一次显式预算清理剩余 3 view 并 finish，canonical 字节计数保持 |
| 事务回滚与未知提交 | Capture／begin／view／finish 的暂存后 trigger 错误；open／close／capture／materialize／begin／view／finish 各覆盖 COMMIT 转发前断开和确认后丢 ACK，全部旧或全部新，按原身份查询或显式继续；materialize 不得发布未经确认的完整副本 |
| 真实服务崩溃 | 实际 SIGKILL PG／MinIO；保留部分回收 intent、已完成回执、剩余 SQL 保护及持久 pin，重开先核对完整状态，再显式恢复；旧 session 不重连，已返回私有 bytes 保持 |

Open／capture 的未知提交也可能留下未交付给调用方的持久 session／pin；测试要求通过原 session ID 查询并显式回收，不能因为本地没有返回值就假定不存在。另给新 oracle 注入错误 fence、缺少 session 绑定、owner 转移和错误副本 owner，要求每个不安全变体有反例。

同时修复 [COMMIT 故障 proxy](../test/vane/postgres_commit_proxy.py) 的长连接清理：仅 `close()` 时，另一转发线程仍阻塞在 recv，可能留下已从代理登记移除但仍持锁的 PG backend。先对自有 socket 双向 shutdown，再 close；新增真实客户端 EOF 验收，要求上游 backend 和 lifetime lock 实际消失。实现没有为此绕过 `READER_ACTIVE` 或添加自动重试。

## 复现与证据

```bash
/path/to/isolated/python -I test/vane/test_vane_vortex_reader_sessions.py \
  --cpus 6,7,8,9,10,11 --pg-bin /path/to/postgresql-15/bin \
  --minio /path/to/pinned/minio --object-data-dir /fresh/objects \
  --output-dir /fresh/reader-proof
```

沿用[固定 requirements](../test/vane/requirements-remote-backend.txt)、PostgreSQL **15.13** 和固定 MinIO SHA。由于本机磁盘余量不足，本轮显式把服务／对象 fixture 放在新的 `/dev/shm` 目录，报告和日志另复制保留到 `/tmp`；没有自动切换路径。[CI](../.github/workflows/VortexRemoteBackend.yml)在既有模型／backend／typed／绑定之后运行 reader 专项并保留 JSON／日志，hosted runner 使用自身可用 CPU，不外推为本地 CPU 6–11 已在 CI 重测。

冻结专项通过 **290 项检查，其中 41 项预期拒绝，19 组故障观测**；包含 reader broker、PG、MinIO **3 次实际 SIGKILL（-9）**及真实 SIGSTOP／SIGCONT。四个不安全 oracle 变体均被拒绝。主报告为 `/tmp/ducklake-vortex-reader-proof-v3-20261107/report.json`，SHA-256 **`2d37766216ea8c70fa4450b5a403c3105b74714da13a77f245d5c5b18a245d7b`**；记录完整最终状态、实际 lock／COMMIT／GET 观测、预算、源码 SHA、客户端版本、CPU／线程环境，不保存可使用的签名票据或 worker 的 stdin 配置。

使用修复后的 proxy 重跑受影响的真实服务回归；全部报告的执行源码与本次提交一致：

| 回归 | 结果 | report.json SHA-256 |
| --- | --- | --- |
| #94 绑定，强制 session／typed v3 | 231 检查／21 拒绝／7 故障 | `1de20f07b2e83c5f93fc1d877a0a2025556666d61fc87de32e8dcf7c980306a6` |
| #93 typed v3 | 121 检查／51 拒绝／9 故障 | `db82b1a032131fe900869f56cfdbf73d749c587048c7b3f51f30823c8b945143` |
| #92 backend | 458 接口／9 能力／7 故障 | `3810dadfacd179bc9d06b2e21e24d37c3ef4ec505008ac1e5c94572692bf7044` |
| #91 模型 | 39 场景／18 不安全变体反例，905＋437 状态，集合摘要保持 | `169453cc67210e4b70aa5afb49b5cf98561272c0ec80815ba10be41e0f0e46b2` |

真实服务回归报告位于 `/tmp/ducklake-vortex-reader-{bound,typed,backend}-regression-v2-20261107/`，模型报告位于 `/tmp/ducklake-vortex-reader-model-regression-20261107/`。#94 场景按新强制 session 契约更新：PG 重启后旧 session 不重连，先前已返回的完整副本保持；旧 pin 经显式原身份回收，新 session 才能发起新的对象读取。原跨 UUID／同名重建、事务保护、预算、manifest／版本闭包和 canonical GC 断言继续保留。

Vane 基础 launcher 在 CPU 6–11、独立非 Ray／Ray 进程通过 **1,222＋32＝1,254 项**，可选依赖缺失跳过 **11＋4＝15 项**；日志 `/tmp/ducklake-vortex-reader-base-20261107.log`，测试 `TMPDIR` 显式放在独立内存目录，线程环境保持。专项 `checkout-audit.json` 核对执行源码、所有报告、原生树／pins 和与 #94 相同的三个已安装 native／provider 构件。原生源码／Vane 包源未改，无 native 重建或重装。格式、文档链接、workflow YAML／bash、copyleft 和 diff 检查通过。

## 剩余边界与下一项

Session 是可信 broker 的对象访问边界，不是针对持有 PG／S3 管理凭证的恶意进程的隔离机制。任意 PG／Vane SQL 事务尚未自动注册成这些 reader；原生事务保护与 native DuckDB／本地 POSIX 限制保持。

下一项推荐 **R6 实际 Vortex／SPFresh 文件与 typed row／ReadView 的编码接线验证**，先证明字段／物理行映射、真实对象闭包与最小查询，再接原生 SQL／普通事务自动登记。真实 AWS／TLS／IAM、HA／掉电、fan-out／读位置等待／分布式 top-k、协议 metadata／STOP 总空间上界及 R7 在线更新仍未完成。
