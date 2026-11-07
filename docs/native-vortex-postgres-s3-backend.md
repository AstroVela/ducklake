# R6 PostgreSQL／S3 最小后端与确定性契约验证

本轮基于 [Draft #91](https://github.com/AstroVela/ducklake/pull/91) 的 `95f82fc574a8e9741f8022d92801fea1e12e26f8`，把[远程协议模型](native-vortex-remote-publication.md)接到真实 PostgreSQL 事务与 S3 API。入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。

后续 [PostgreSQL 类型化 metadata 与 UUID／epoch 契约](native-vortex-postgres-typed-lifecycle.md)已新增独立 typed 组件；[typed UUID、发布与 ReadView 统一绑定](native-vortex-bound-readviews.md)进一步连接 source／generation／引用／退休，采用全新 v2 布局。本文保留 #92 的发布 adapter 基线与历史结果，原生远程 SQL／ANN 接线仍需后续完成。

交付的是可独立调用的[实验性 backend](../scripts/vortex_remote_backend.py)及真实服务 conformance，**尚未接入 Vane SQL、原生维护调度或 ANN 查询**。生产 C++／Rust、native DuckDB 限制、SQL 接口与 provider 保持 #91；现有原生普通事务保护不变。测试使用独立私有 schema 和受登记约束的 S3 namespace，上传受控 opaque 字节与真实 manifest；不把它们当作真实 Vortex／SPFresh 编码资格。

本地固定 CPU **6–11**，继承现有线程环境；原生测试的 `threads=2` 保持。PostgreSQL、MinIO 和故障线程继承同一 affinity。现有预算不扩大，不增加兼容层、第二份 WAL、自动重试或本地 fallback。R4 A/A、性能 A/B、长期稳态及依赖稳定负载的 R5 资格继续暂缓；时间只作观察。

## 单一授权配置：可信 broker 与签名票据

实测固定的 MinIO `RELEASE.2025-09-07T16-13-09Z` 拒绝 IAM 条件键 `s3:if-none-match`。本轮明确选择 **SigV4 签名票据作为唯一 worker 授权方式**，不宣称已通过该 IAM 条件策略。Broker 持有 PostgreSQL 和 S3 凭证；worker API 只接收一个 `Ticket(method, key, url, headers)`。

1. Broker 先在 PostgreSQL 事务中锁 owner、核对请求／对象登记，再签发票据；提交成功后才能把票据交给 worker。
2. Payload PUT 绑定 bucket、key、Content-Length、SHA-256 checksum 与 `If-None-Match: *`；STOP PUT 绑定固定 body 与所探测 payload 的 ETag 条件，或不存在条件。
3. DELETE 票据绑定一个已登记 payload version ID。Worker 改 key、方法、条件头或删除版本会破坏签名；修改 payload 会破坏 checksum。代码还独立检查 SDK 的 SigV4 模式、必需 signed headers 与精确删除参数。
4. Worker 只执行一次 HTTP 请求。未知响应、条件失败和过期票据都返回错误，由当前 owner 显式核对／续作；不隐式重签、改请求身份或扫描重试。

AWS 的 SigV4 会校验请求中的方法、URI、查询参数与签名头；presigned 请求的能力受签名者原有权限约束。[AWS SigV4 查询参数认证](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html)、[Presigned URL 说明](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)

票据暂设有效期 300 秒，**GC 不依赖票据过期**，仍永久保留普通 STOP 屏障。签名 URL 本身是可使用的授权，证据不记录 URL、签名或 secret。测试通过真实服务拒绝篡改来验证票据边界；broker／worker 的生产进程权限隔离、凭证轮换、TLS 部署和云 IAM 资格仍待后续验证。可信 broker 的凭证不能传给 worker；绕过 broker 使用管理凭证删除 STOP 不属于已验证的故障模型。

## 持久元数据与存储绑定

仅支持一个私有 v1 schema、一个 catalog、一个固定表 UUID／request epoch 与一个命名索引。初始化必须创建全新 schema；没有迁移或旧布局兼容。

| PostgreSQL 表 | 持久内容 |
| --- | --- |
| `control` | layout、catalog／table UUID、request epoch、owner epoch、named revision／head，以及固定 endpoint／bucket／prefix |
| `units`／`objects` | source／generation 登记、原始收费字节、生命周期、key／role、精确 version ID、尺寸、SHA 和封存状态 |
| `requests` | 不变的 request ID、payload hash、source、expected revision；可接管 claim、状态、manifest 与原回执 |
| `refs`／`readers` | native 历史、named root、build 输入、reader pin，以及所捕获的同一 root／revision |
| `gc` | 原 unit、稳定 nonce、当前 claim、逐 key payload target、STOP 确认与一次完成回执 |

每个 metadata 变更事务都按 `SELECT control … FOR UPDATE` 串行化，使用 READ COMMITTED、`synchronous_commit=on`，在同一行锁下核对 owner、布局和存储绑定。初始化还要求 `fsync`、`full_page_writes` 开启。Broker 之间使用真实数据库锁，不用 Python 进程锁替代。普通查询可独立读取旧的完整 committed 状态。[PostgreSQL 行锁](https://www.postgresql.org/docs/15/explicit-locking.html)、[事务持久性配置](https://www.postgresql.org/docs/15/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT)

Endpoint、bucket 和 prefix 随 catalog 持久登记，并进入 manifest 身份；重开配置不匹配时，在签发对象请求或修改 owner 前拒绝。单有相同 key／version 字符串不允许把元数据转接到另一个 bucket 或服务。

请求 hash 包含 source、index 内容身份、行数、维度与段数。重试保留原 expected revision；adopt 只改 claim。head、完整 named／build 引用变更、generation 状态和请求回执在一个 PostgreSQL 事务内提交。COMMIT 已发出但网络断开时抛出 `CommitUnknown`，保留已上传字节，调用方用原请求身份查询回执；没有自动物理清理或第二份客户端日志。

## 对象、读者与 GC

Source 先登记并持 build 引用，再条件上传、完整读回校验、封存精确版本并转为 native 引用。Generation 上传／封存 index 后，manifest 绑定 store、UUID／epoch／request、原 expected revision、输入 snapshot 与 source／index 的精确版本。发布只使用完整封存的候选；取消解除 build 引用，不直接删除对象。

S3 payload 与 manifest 通过实际版本 GET 读回，重新校验尺寸和 SHA；ETag 只用作条件令牌。普通请求必须为 HTTPS，本地 fixture 只允许 loopback HTTP。未启用 versioning、存在启用中的 lifecycle、SDK 自动重试或非 SigV4 配置均不能进入对应操作；没有功能降级。

捕获视图在同一个 control 行锁事务中固定 root／revision 并登记全部 pin。Owner 接管不删除 reader。`materialize` 按精确版本取得完整私有 bytes 并校验后，才允许显式解除 pin；旧规范对象被删除后，副本保持。没有 TTL 自动释放。当前 adapter 的普通 SQL 保护使用显式 reader session 登记：活跃 `kind=sql` session 全局阻止 native 历史释放和 source GC。**它尚未自动接管任意 PostgreSQL 或 Vane SQL 事务**；原生 transaction-start gate 保持原实现，原生入口继续拒绝远程配置。

GC 只接纳无 named、native、build 或 reader 引用的已登记 unit。DELETING／intent 提交后，逐 key 探测并保存 exact target，签发普通 STOP 条件 PUT；条件失败由当前 owner 重新探测同一 intent。确认 STOP 后，签发仅能删除原 payload version 的票据，最后核对全部屏障及版本不存在，再原子完成计账。Adopt 保留 nonce 和删除目标；已签发的旧 PUT／STOP／DELETE 可晚到，但不能改变其他版本或复活 payload。

Broker 的恢复与 GC 不调用对象 LIST，也不猜测 orphan。测试 oracle 使用 LIST 全量审计，属于独立验证。STOP 保留与 protocol metadata 累积仍无总空间上界。

## 预算

继续使用 source／artifact／vector 各 64 MiB、manifest 1 MiB、最多 100,000 行、维度 1–4096、65 行静态构建下限和最多三个 B 的 admission 常量。首版 backend 的 B 是一个 opaque generation，段数／向量尺寸是候选输入约束；这不验证真实三段 ANN 编码、全局查询资源或分布式 admission。

GC 每轮最多 16 个逻辑 unit、64 MiB，按登记原始费用保守接纳，恢复重新预留，完成回执只记一次。测试包含一轮 16／17 个 unit、恰好 64 MiB／少 1 字节及共享字节预算耗尽。64 MiB 用例登记后取消、尚未上传该 payload，只写小型 STOP；报告中的 64 MiB 是原始 admission charge，不代表实际删除了 64 MiB 对象。协议 metadata、STOP、SDK、数据库 WAL 和进程 RSS 不由该计数代表。

本版每个 `Budget` 由单一 collector 顺序使用；它不是跨进程共享 admission 服务。原生 maintenance 的单 worker／全局预算继续保持，尚未改由此实验性 backend 调度。

## 独立验证与故障边界

[Coordinator](../test/vane/test_vane_vortex_remote_backend.py) 启动全新 PostgreSQL／MinIO，使用[独立 oracle](../test/vane/vortex_remote_backend_oracle.py)直接读取七张 PostgreSQL 表和所有 S3 版本／实际字节。Oracle 不调用 backend 的引用、封存、manifest 或回收判定；核对 head／回执链、完整引用集、对象 SHA／版本、manifest lineage、原回执、未登记邻居及已回收对象不复活。预期拒绝必须保持 metadata 与对象快照不变。

| 边界 | 固定握手与断言 |
| --- | --- |
| 签名票据 | 省略／更改条件头、改 key、改方法、改 checksum body、把 exact DELETE 指向 STOP 版本均被服务拒绝；正向精确 GET／DELETE 成功 |
| S3 delete marker | 在独立 capability key 上实测旧条件上传可成功复活；随后只清理 fixture 自己的 probe 版本 |
| 晚到上传 | HTTP proxy 已完整收到 PUT，暂停转发；取消、接管并完成 GC 后才放行，必须只有一次请求并返回 412 |
| HEAD／STOP 竞争 | 探测不存在并签发 STOP 后先投递旧 payload；旧 STOP 返回 412，显式重新探测并完成原 intent |
| PUT／DELETE 响应丢失 | 上游真实返回 200／204 后 proxy 丢弃响应；精确身份核对后显式续作，没有再次发请求来选结果 |
| PostgreSQL COMMIT | 复用[协议帧 proxy](../test/vane/postgres_commit_proxy.py)，分别在转发前断开、收到服务端 `COMMIT`＋ReadyForQuery `I` 后丢响应；独立观察必须是全旧或全新状态 |
| 事务内错误 | 测试 trigger 在 head／refs 更新之后、回执提交之前抛错；全部七表状态和 S3 版本恢复原样 |
| 两个 broker CAS | 测试 advisory gate 加 `pg_stat_activity` 锁等待握手，固定一个 winner；独立 observer 看不到中间态，另一个明确 revision conflict |
| 服务崩溃 | 在持久 GC intent 和已落 STOP 后，实际 SIGKILL 两个自有服务；重启核对完整状态，再接管并显式完成 GC；旧精确 DELETE 安全、旧 STOP 失败，reader pin 继续可读 |

所有故障由事件、协议帧或数据库锁状态选择；sleep 只用于 readiness／握手轮询，不以持续时间挑选崩溃窗口。Fixture 只关闭自己启动的进程，不复用或改动已有服务。

## 复现与 CI

```bash
uv venv /tmp/r6-backend-env
uv pip install --python /tmp/r6-backend-env/bin/python \
  -r test/vane/requirements-remote-backend.txt
/tmp/r6-backend-env/bin/python -I test/vane/test_vane_vortex_remote_backend.py \
  --cpus 6,7,8,9,10,11 --pg-bin /path/to/postgresql-15/bin \
  --minio /path/to/pinned/minio \
  --object-data-dir /fresh/object-data --output-dir /fresh/backend-proof
```

本地使用 PostgreSQL **15.13** 和 MinIO **RELEASE.2025-09-07T16-13-09Z**，MinIO SHA 为 `7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f`。客户端由[独立 requirements](../test/vane/requirements-remote-backend.txt)固定，不加入 Vane 或 provider 依赖。本机磁盘接近满，MinIO 曾实际返回 `XMinioStorageFull`；最终显式指定新的 `/dev/shm` fixture 目录，测试结束后删除该自有目录。未修改服务磁盘保护，也未自动切换目录或设置线程绕过变量。

[独立 CI](../.github/workflows/VortexRemoteBackend.yml)固定 action 和 MinIO 构件，安装 PostgreSQL 15，选择 runner 可用 CPU，依次运行 #91 模型与真实 backend conformance，保留 rule／report 和服务日志；不运行性能或长期负载。报告记录实际 PostgreSQL minor、客户端与执行源码身份，不能把本地 15.13 结果外推成所有 minor 均已通过。

## 本轮冻结结果

冻结专项通过 **458 项接口契约检查**，其中 26 项预期拒绝核对完整状态不变；另外通过 **9 项服务能力检查、7 组故障／并发观测**。PostgreSQL 和 MinIO 实际 SIGKILL 的退出码均为 **-9**；PostgreSQL 日志出现 WAL redo，重启后的 metadata／对象版本与崩溃前精确一致，随后显式完成原 GC intent。重启服务的 CPU affinity 仍为 6–11。原始副本、邻居对象、回执与固定 ReadView 的独立断言均通过。

Backend 主报告为 `/tmp/ducklake-vortex-r6-backend-proof-v2-20261107/report.json`，SHA-256 **`80cbf5bfb5c478efe50f784b0389052de041e2942d83493128a55099aafd871d`**。报告记录全部检查名／状态摘要、负向结果、S3 上游状态与请求次数、PostgreSQL COMMIT 协议观测、故障退出码、最终七表／版本清单、客户端版本、CPU／线程环境与执行源码 SHA；不包含可使用的签名票据。源码执行前后及冻结后复核一致，affinity 已恢复。

#91 的模型回归保持 **39 场景、18 个不安全变体反例、905＋437 个有界状态**全部通过；状态集合摘要与 #91 相同。报告为 `/tmp/ducklake-vortex-r6-model-proof-20261107/report.json`，SHA-256 `7b17e499882bce102e58ad232f19baab3fdc0eb9429c0afcbe565cb6244510ef`。

Vane 原基础 launcher 按独立非 Ray／Ray 进程，在 CPU 6–11 通过 **1,222＋32＝1,254 项**，可选依赖缺失跳过 **11＋4＝15 项**；日志为 `/tmp/ducklake-vortex-r6-backend-base-20261107.log`。格式、文档链接、workflow YAML、copyleft 与 `git diff --check` 通过。专项目录的 `checkout-audit.json` 核对 #91 的原生源码／依赖 pin，以及相同的已安装 `_native`、DuckLake 和 Vortex artifact SHA；本轮没有修改 Vane 包源或重建／重装 native provider。

真实 AWS bucket、跨主机网络、TLS／IAM 部署、数据库 HA／掉电持久性、reader 故障回收、UUID／epoch 推进、类型化 DuckLake metadata、原生远程 SQL 与 ANN 仍未完成。

下一项推荐先做 **R6 PostgreSQL 类型化 metadata 与 UUID／epoch 生命周期契约验证**，随后补 reader session fencing／pin 回收并把 broker 边界接到原生调用。满足这些前置条件后再解除 native 限制；固定 ReadView fan-out、读位置等待与分布式 top-k 继续独立验收，R7 SPFresh 在线更新仍未接入。
