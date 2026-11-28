# R6 Reader session 所有权、fencing 与持久 pin 回收

本轮基于 [#94 合入提交](https://github.com/AstroVela/ducklake/commit/d21d4609e7e78dd365df587e20a2582162a8f0d1) `d21d4609`，在 [UUID／发布／ReadView 绑定](native-vortex-bound-readviews.md)上增加必须显式携带的 reader session。总入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。

本地保持 CPU **6–11**、继承的线程环境、原生 `threads=2`、事务保护和预算。R4 A/A、性能 A/B、长期稳态及依赖稳定负载的 R5 资格继续暂缓，时间只作观察。本轮是实际 PG／MinIO 的组件契约验证；source／index 仍为 opaque fixture，未接入原生 Vortex／SPFresh 编码或远程 ANN。

## 明确的一次会话身份

[ReaderSession／ReaderReclaimer](../scripts/vortex_reader_sessions.py)与 [BoundCatalog](../scripts/vortex_bound_catalog.py)使用新初始化的 PostgreSQL schema。Storage 和 typed 都是 **`layout=1`**。新建 schema 时创建以下三张表。

| 持久表 | 内容 |
| --- | --- |
| `reader_sessions` | Session UUID、owner UUID、advisory key、PG backend PID／start、fence、状态、原 intent／完成回执 |
| `reader_session_views` | 每个 typed ReadView 绑定唯一 session UUID／原 fence，与 capture 的 root、snapshot 和全部 pin 同事务提交。每个视图另保存一个从零开始的持久验收序号 |
| `reader_reclaims` | 逐视图释放的原回执，包括 session／fence、实际释放 unit 集、零 payload bytes；复合外键限制所属 session／view |

Session UUID 不复用。一个 incarnation 从 fence **1** 的 active 开始，只能正常关闭，或经过显式回收进入 terminal fence **2**；没有猜测新 incarnation 或循环使用旧 ID。业务 UUID／request epoch 与 reader fence 是不同身份。

`open_session` 创建一条专用 PG 连接，持有由 catalog UUID／session UUID 派生的独占 session-level advisory lock，并持久化实际 PID／backend start。Session-level 锁随服务端会话结束释放；事务级 advisory 锁可用于不等待地探测竞争，行锁继续保护 metadata 原子变更。[PG advisory locks](https://www.postgresql.org/docs/15/explicit-locking.html#ADVISORY-LOCKS)、[锁函数](https://www.postgresql.org/docs/15/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)

普通 reader RPC 在独立 metadata 事务中取得既有 `control … FOR UPDATE`，同时核对 session 状态／fence／owner、PID／start 和实际锁所有权；`pg_locks` 的数据库、key 高低位及锁类型全部匹配。[PG 锁视图](https://www.postgresql.org/docs/15/view-pg-locks.html) 锁 key 冲突会拒绝创建，不寻找替代 key。

## 对象访问与私有副本

Capture、materialize、解除 copy pin 和关闭视图都必须携带所属 session。跨 session 的 materialize／close／unpin 在访问 S3 前拒绝；writer owner 轮换不撤销仍活跃的独立 reader。

Materialize 使用独立 PG 连接，并以同一 key 的两个 32 位整数获取 session 级 operation lock。该锁与用单个 bigint 标识的 lifetime lock 位于不同的 advisory 锁空间。操作连接存活时，operation lock 覆盖读取、两个 COMMIT 确认和副本登记。连接终止即释放该锁。[PG advisory 锁函数](https://www.postgresql.org/docs/15/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)

首个短事务在 control 行锁内固定 typed 行、视图和计划读取的精确对象版本。每次 GET 通过单独的短事务授权，在同一行锁内核对 session、实际 lifetime lock、视图和完整 pin。授权事务正常结束后，broker 用 SigV4 为该对象的 version ID 签名，通过单次 HTTP 请求读取。区域错误和 HTTP 重定向直接终止当前物化。网络读取在全局行锁外完成，沿用 S3 客户端的连接与读取超时和原对象字节预算。每个读取返回后再次核对实际 lifetime lock，并验证版本、数据摘要和大小。

物化完成读取后在 control 行锁内重新校验视图并 COMMIT。收到这次确认后，验收事务在同一行锁内再次核对持久 fence、实际 lifetime lock、视图和 pin，并原子递增该视图的验收序号。只有收到验收 COMMIT 确认的调用才登记、交付私有副本，副本携带本次的 `acceptance` 序号。验收确认丢失时，序号可能已推进，持久 pin 保留，本次副本不交付。验收只记录序号，副本数据仍保存在私有内存。

显式回收和正常关闭 session 都先等待目标 session 的 operation lock，再取得全局 control 行锁。等待期间其他表可以提交。关闭单个视图或解除 pin 在验收事务前提交时会阻止验收，包括首次物化 COMMIT 已提交且确认仍在传输时。Fence 推进后，旧 session 无法获得任何新授权或提交新验收。若操作连接终止，回收可以先完成；此前已授权的那一次精确版本 GET 可能在 fence 推进后才发出或返回，旧调用无法继续下一次授权或验收。此前已持久验收且获得确认的副本可以在 fence 推进后登记、返回，并继续读取。

`disconnect` 只关闭自己的 lifetime 连接，不修改持久 pin。正常 `close_session` 要求所属视图全部关闭。成功提交后在同一 operation lock 的保护下断开 lifetime 连接。关闭的 COMMIT 不确定也会断开该连接，后续通过原 session 身份核对结果或显式回收。

## 显式、受预算的回收

| 步骤 | 原子边界 |
| --- | --- |
| `begin(session_uuid, original_fence, owner_token, budget)` | 先等待 session 的 operation lock，再持有 control 行锁复核当前 owner 和布局，最后尝试取得 lifetime key 对应的事务级 advisory lock。活跃 holder 返回 `READER_ACTIVE`。取得锁后只持久化 intent 并推进 fence |
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
| GET 授权与断连 | 在真实 GET 已完成且结果尚未返回时终止 lifetime backend，确认回收等待 advisory lock，另一张表仍可提交。同时终止操作 backend 后，回收可先推进 fence。另在授权已完成、GET 尚未发送时终止两条连接，等回收完成后再放行，验证只发送原精确版本的一次 GET，无法发出下一次或交付副本 |
| 区域错误与重定向 | 对真实 MinIO 使用错误签名区域，从客户端 HTTP 发送层确认只发送一次带精确 version ID 的签名 GET。返回 400 后验收序号仍为零且 pin 保留。隔离副本恢复原 SDK GET 时，该断言捕获两次发送。本地 HTTP fixture 返回 307 时复用同一计数。隔离副本在 307 后额外发出 HEAD 时，断言记录 GET 和 HEAD，并因两次发送而失败 |
| 正常关闭与 COMMIT 确认 | 暂停真实 GET，验证正常关闭等待目标 session 且另一张表可提交。关闭已提交且确认仍暂停时，旧 session 无法获得 GET 授权。在首次物化 COMMIT 确认暂停时通过第二个 BoundCatalog 先关闭视图或解除 pin，确认恢复后验收序号保持原值 |
| 已验收副本晚于 fence 返回 | 在验收 COMMIT 已确认且副本尚未登记时终止操作 backend，另一 BoundCatalog 关闭视图和 session。放行后按原验收序号交付同一视图、同一 payload 的副本。终态后重新物化拒绝，序号保持不变 |
| 所有权与约束 | 跨 session 的读取／unpin／close／回收、重复 UUID、非法 fence、旧 writer、复合外键及 CHECK 拒绝。Begin、view 和 finish 拒绝无效 writer owner，且保持持久状态和预算。Writer 轮换后活跃 reader 仍可 materialize |
| 全局预算 | 17 个 view；同一预算先登记一个 canonical GC unit、一个 session intent，再释放 14 个 view，恰好 16 项；下一项拒绝。另一次显式预算清理剩余 3 view 并 finish，canonical 字节计数保持 |
| 事务回滚与未知提交 | Capture／begin／view／finish 的暂存后 trigger 错误。Open／close／capture／首次物化校验／副本验收／begin／view／finish 各覆盖 COMMIT 转发前断开和确认丢失。验收转发前断开时序号保持，提交后丢确认时序号推进，两个场景的 pin 均保留且副本均未交付 |
| 真实服务崩溃 | 实际 SIGKILL PG／MinIO；保留部分回收 intent、已完成回执、剩余 SQL 保护及持久 pin，重开先核对完整状态，再显式恢复；旧 session 不重连，已返回私有 bytes 保持 |

Open／capture 的未知提交可能留下未交付给调用方的持久 session／pin，测试通过原 session ID 查询并显式回收。另给新 oracle 注入错误 fence、缺少 session 绑定、owner 转移、错误副本 owner 和未验收副本，要求每个不安全变体有反例。

同时修复 [COMMIT 故障 proxy](../test/vane/postgres_commit_proxy.py) 的长连接清理：仅 `close()` 时，另一转发线程仍阻塞在 recv，可能留下已从代理登记移除但仍持锁的 PG backend。先对自有 socket 双向 shutdown，再 close；新增真实客户端 EOF 验收，要求上游 backend 和 lifetime lock 实际消失。实现没有为此绕过 `READER_ACTIVE` 或添加自动重试。

## 复现与证据

```bash
/path/to/isolated/python -I test/vane/test_vane_vortex_reader_sessions.py \
  --cpus 6,7,8,9,10,11 --pg-bin /path/to/postgresql-15/bin \
  --minio /path/to/pinned/minio --object-data-dir /fresh/objects \
  --output-dir /fresh/reader-proof
```

沿用[固定 requirements](../test/vane/requirements-remote-backend.txt)、PostgreSQL **15.13** 和固定 MinIO SHA。由于本机磁盘余量不足，本轮显式把服务／对象 fixture 放在新的 `/dev/shm` 目录，报告和日志另复制保留到 `/tmp`；没有自动切换路径。[CI](../.github/workflows/VortexRemoteBackend.yml)在既有模型／backend／typed／绑定之后运行 reader 专项并保留 JSON／日志，hosted runner 使用自身可用 CPU，不外推为本地 CPU 6–11 已在 CI 重测。

### 当前开发基线验收

全部真实服务回归使用新建 `layout=1` schema、CPU 6–11 和既有线程环境。各套在 180 秒期限内完成。

| 验证 | 结果 |
| --- | --- |
| Reader session | 351 项检查，含 57 项预期拒绝。28 组故障观测、5 个不安全变体反例 |
| 绑定 | 373 项检查，含 76 项预期拒绝。10 组故障与并发观测 |
| Typed | 175 项检查，含 91 项预期拒绝。9 组故障观测 |
| 后端 | 485 项检查，含 35 项预期拒绝。9 项能力探测、7 组故障观测 |
| 协议模型 | 40 个场景、21 个反例，1,342 个状态的集合摘要与开发基线一致 |

三组在途 GET 用例均观察到回收等待目标 session 的 advisory lock，且另一张表在等待期间提交。各组只完成一次精确版本 GET，均未登记副本。其中一组在回收完成、fence 已为 2 时才发出此前已授权的 GET。真实 MinIO 的区域错误和 HTTP 307 各观察到一个实际请求。跨实例关闭视图和解除 pin 都能让尚未验收的物化失败。已获确认的第二份副本在另一个实例关闭会话、fence 推进后按验收序号 2 返回。最终验收 COMMIT 转发前断开和提交后丢确认的两组故障均保留全部 pin，本次副本均未登记。共享预算的 16 个工作项上限和 payload 字节计数通过验证。

### 原叠栈验收

原 PR 基于 #94 的旧叠栈提交 `5b7c01b3`。当时 storage 为 v2、typed 为 v3，专项通过 **290 项检查，其中 41 项预期拒绝，19 组故障观测**；包含 reader broker、PG、MinIO **3 次实际 SIGKILL（-9）**及真实 SIGSTOP／SIGCONT。四个不安全 oracle 变体均被拒绝。主报告为 `/tmp/ducklake-vortex-reader-proof-v3-20261107/report.json`，SHA-256 **`2d37766216ea8c70fa4450b5a403c3105b74714da13a77f245d5c5b18a245d7b`**；记录完整最终状态、实际 lock／COMMIT／GET 观测、预算、源码 SHA、客户端版本、CPU／线程环境，不保存可使用的签名票据或 worker 的 stdin 配置。

原叠栈使用修复后的 proxy 运行了以下前序回归。

| 回归 | 结果 | report.json SHA-256 |
| --- | --- | --- |
| #94 绑定，强制 session／typed v3 | 231 检查／21 拒绝／7 故障 | `1de20f07b2e83c5f93fc1d877a0a2025556666d61fc87de32e8dcf7c980306a6` |
| #93 typed v3 | 121 检查／51 拒绝／9 故障 | `db82b1a032131fe900869f56cfdbf73d749c587048c7b3f51f30823c8b945143` |
| #92 backend | 458 接口／9 能力／7 故障 | `3810dadfacd179bc9d06b2e21e24d37c3ef4ec505008ac1e5c94572692bf7044` |
| #91 模型 | 39 场景／18 不安全变体反例，905＋437 状态，集合摘要保持 | `169453cc67210e4b70aa5afb49b5cf98561272c0ec80815ba10be41e0f0e46b2` |

真实服务回归报告位于 `/tmp/ducklake-vortex-reader-{bound,typed,backend}-regression-v2-20261107/`，模型报告位于 `/tmp/ducklake-vortex-reader-model-regression-20261107/`。#94 场景按新强制 session 契约更新：PG 重启后旧 session 不重连，先前已返回的完整副本保持；旧 pin 经显式原身份回收，新 session 才能发起新的对象读取。原跨 UUID／同名重建、事务保护、预算、manifest／版本闭包和 canonical GC 断言继续保留。

Vane 基础 launcher 在 CPU 6–11、独立非 Ray／Ray 进程通过 **1,222＋32＝1,254 项**，可选依赖缺失跳过 **11＋4＝15 项**；日志 `/tmp/ducklake-vortex-reader-base-20261107.log`，测试 `TMPDIR` 显式放在独立内存目录，线程环境保持。专项 `checkout-audit.json` 核对执行源码、所有报告、原生树／pins 和与 #94 相同的三个已安装 native／provider 构件。该次运行的原生源码和 Vane 包源沿用原叠栈基线。格式、文档链接、workflow YAML／bash、copyleft 和 diff 检查通过。

## 剩余边界与下一项

Session 是可信 broker 的对象访问边界，不是针对持有 PG／S3 管理凭证的恶意进程的隔离机制。任意 PG／Vane SQL 事务尚未自动注册成这些 reader；原生事务保护与 native DuckDB／本地 POSIX 限制保持。

下一项推荐 **R6 实际 Vortex／SPFresh 文件与 typed row／ReadView 的编码接线验证**，先证明字段／物理行映射、真实对象闭包与最小查询，再接原生 SQL／普通事务自动登记。真实 AWS／TLS／IAM、HA／掉电、fan-out／读位置等待／分布式 top-k、协议 metadata／STOP 总空间上界及 R7 在线更新仍未完成。
