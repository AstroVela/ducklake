# R6 PostgreSQL 类型化元数据与 UUID／epoch 生命周期契约

本轮基于 [Draft #92](https://github.com/AstroVela/ducklake/pull/92) 的 `44b3c16e8d93f9d4beed2f406bb210dece32c4c1`，新增可独立调用的 [typed metadata 组件](../scripts/vortex_postgres_metadata.py)，复用 [PostgreSQL／S3 broker](native-vortex-postgres-s3-backend.md) 的 owner／事务边界，并在真实 PostgreSQL 上验证固定布局与生命周期。总入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。

**本轮资格限于 broker 组件层。** 原生 C++／Rust、现有 DuckLake inline codec、native DuckDB 限制、SQL 接口、provider 和 #92 发布 adapter 均不修改。Vane 远程 SQL／ANN、实际 source／generation 与 typed UUID 的统一绑定仍未接入。新组件不读旧 typed 布局、不迁移、不自动转换 UUID／epoch，也不新增兼容路径或 fallback。

本地保持 CPU **6–11**、原线程环境、原生 `threads=2`、既有事务保护和预算。原生 maintenance 的单 worker／全局预算保持。R4 A/A、性能 A/B、长期稳态和依赖稳定负载的 R5 资格继续暂缓；时间只作为观察值。

## 原生路径审计与本轮范围

[PostgresMetadataManager](../src/metadata_manager/postgres_metadata_manager.cpp) 已把 FLOAT 映射为 REAL、DOUBLE 映射为 DOUBLE PRECISION，VARCHAR／BLOB 使用 BYTEA；LIST 被标为非 native 类型。[通用 metadata manager](../src/storage/ducklake_metadata_manager.cpp) 的 `GetColumnType` 会把非 native 的嵌套类型表示为 VARCHAR。因此，新的 REAL[] 组件验证**不等于现有 C++ LIST 文本 codec 已通过远程资格**，后续接线必须明确处理边界。

PostgreSQL BIGINT 是有符号 64 位，无法表示完整的正 uint64 epoch；NUMERIC 可精确保存该范围。PG 数组的声明不自动强制维度及长度，TEXT 不能存 NUL，因此需要显式的数组约束与字节表示。[PG 数值类型](https://www.postgresql.org/docs/15/datatype-numeric.html)、[PG 数组](https://www.postgresql.org/docs/15/arrays.html)、[PG 字符类型](https://www.postgresql.org/docs/15/datatype-character.html)

本轮固定一个新 typed-v1 布局，用参数化 psycopg SQL 访问真实 PG 类型；不为不同 PG／DuckDB 编码提供选择分支。测试 schema 增加可空标量、BLOB 和通用列表探针；它不是把原生四业务列查询扩展为通用 schema 支持。

## 固定类型与身份

| 逻辑值 | PG 表示与约束 | 验证 |
| --- | --- | --- |
| 表 UUID | UUID；历史 UUID 不复用，活跃名称唯一 | 同名重建、旧 UUID 写入／推进／退休全部拒绝 |
| 字段身份 | BIGINT field ID＋ordinal＋逻辑类型＋nullability；固定 schema SHA | 顺序、非连续 ID、同名新 UUID 的旧字段绑定拒绝 |
| 请求 epoch | NUMERIC(20,0)，CHECK `1..18446744073709551615`；API 仅接受整数 | 跨 `2^63`、uint64 最大值、最后一次推进重试与耗尽拒绝 |
| 行键／物理行地址 | BIGINT；物理 row ID 单调分配，UUID＋快照限定版本 | 最小／最大 BIGINT、替换／删除、历史视图与退休后不复用 |
| 有限向量 | REAL[]，显式一维、下界 1、维度匹配、非 NULL 元素、无 NaN／Inf | 1／2／3／4096 维、signed zero、最小次正规数、最大有限值、非法数组拒绝 |
| 可空 FLOAT／DOUBLE | REAL／DOUBLE PRECISION | OID 与 `float4send`／`float8send` 字节；NULL、signed zero、次正规数 |
| 可空业务字符串 | 严格 UTF-8 编码的 BYTEA | NUL、中文／重音／引号、空串与 NULL；拒绝无效 Unicode |
| BLOB／可空通用 LIST | BYTEA／REAL[] | 原始二进制、NULL／空字节，NULL／空列表／含 NULL 元素列表分别保持 |

Schema hash 包含 field ID、ordinal、名称、逻辑类型、可空性和向量维度；请求 hash 使用单一版本的带长度／NULL 标记的二进制表示，按业务键排序，Float32／Double 使用网络序 IEEE 字。NULL、空值和 signed zero 不混同。没有文本解析失败后再试另一种表示的路径。

固定 fixture 的 schema SHA 为 `275033cd1b8b6867acf4d8c0e3825a8fa1cb245806626ee2341a8d693e4fb6cc`，请求 SHA 为 `7cc8a1ee2223fa3b810cf8a0842976765d03582accf02d4ecea5919479ecc0ef`；同时要求 IEEE golden words `3dcccccd`、`80000000`、`00000001`、`3fb999999999999a`。它们约束协议本身，防止两边实现同时漂移后仍互相“验证通过”。

## 事务、窗口与退休

新增五张独立表：`typed_catalog` 保存布局／快照和分配计数；`typed_tables` 保存 UUID、名称、schema／维度、生命周期与当前窗口；`typed_fields` 保存不可变字段身份；`typed_rows` 保存 typed 行及 begin／end snapshot；`typed_receipts` 保存三元请求身份及原始回执。初始化必须创建新布局，没有 schema migration。

所有写操作复用 #92 `control … FOR UPDATE`，同一事务内检查 owner fence、store binding、实际目标 UUID、窗口与 schema hash。写批次把旧行版本关闭、新 typed 行、物理 row ID／快照分配和请求回执一起提交；内部提交重试为零。没有实际数据效果的 delete 仍有操作数与回执，可沿用现有 snapshot。

请求身份为 `(table_uuid, request_epoch, request_id)`。当前窗口的相同 typed 内容返回原回执，改变内容失败；重试不复活已经被后续操作删除的行。推进只清除目标 UUID 的当前窗口回执并原子保存下一窗口和最近一次推进结果，不改业务行、物理版本或其他 UUID。最近一次推进重试返回原结果，保留新窗口已提交回执；更早的推进重试失败。旧 epoch，包括从未提交过的 ID，均拒绝；future／零／非整数／越界 epoch 也拒绝。最大 uint64 不回绕。

退休在一个事务内结束目标 UUID 的活跃行版本，释放其 managed 标记、epoch／最近推进状态和回执；保留表／字段／行的历史以供确切 snapshot 读取和退休回放认证。活跃名称消失后，同名新 UUID 可以从 epoch 1 使用同一 request ID。旧 UUID 不能被新名称目标接管；旧字段绑定也不能用于新 UUID。真实旧 PostgreSQL REPEATABLE READ 事务仍能读其原 tuple snapshot，显式 `read_at(old_uuid,snapshot)` 继续读取保留历史。

这些动作只操作新 typed registry；#92 的 publication fixture UUID／epoch 和 source／generation 引用保持原状，不能据此宣称 typed 退休已经释放真实索引引用。历史物理清理、reader session 回收和端到端原生退休仍需后续统一身份接线。本轮不建立独立永久 tombstone 或总磁盘／WAL／RSS 上界。

## 预算与独立验收

保持每批 **1–1,000 操作／4 MiB**、catalog 合计 **100,000 回执**；当前 UUID 的精确重试在满额时仍可返回原结果。新 typed 组件另按既有 envelope 检查 100,000 活跃行、64 MiB 向量与维度 1–4096。原生 T/F、source／artifact／manifest、三 B、GC 16 对象／64 MiB 等预算未改变；该组件尚未替代原生 admission 或 optimizer。

[独立 oracle](../test/vane/vortex_postgres_metadata_oracle.py)不调用组件的规范化、编码、hash、窗口或退休函数：直接读取五张 PG 表、列 OID、服务端 IEEE 字节与有序回执集合 SHA，对照单独的版本历史模型。[Coordinator](../test/vane/test_vane_vortex_postgres_metadata.py)每次也核对 #92 七张表和所有 S3 版本／实际字节，要求原发布、命名／native／reader 引用和对象保持；只有显式 owner 接管允许改变原 owner 一项。

| 验收组 | 固定条件 |
| --- | --- |
| 类型／拒绝 | 真实 PG OID、IEEE golden、binary cursor 回读、UTF-8／NUL／BLOB、NULL／空值、字段绑定和直接 SQL CHECK 拒绝；预期拒绝前后完整状态相同 |
| 窗口／容量 | 恰好 4 MiB 可提交、超过则拒绝；100,000 回执下真实重试和合规 seeded 重试成功，新请求跨表拒绝；推进／退休释放逻辑槽位 |
| 不确定 COMMIT | mutation／advance／retire 分别覆盖 COMMIT 转发前断开、服务端确认后丢 ACK，必须全旧或全新；随后按原 UUID／epoch 显式重试 |
| 事务内错误 | 三类操作分别在行暂存、回执清理、退休状态更新边界由测试 trigger 抛错，恢复全部 typed 表、计数与原对象状态 |
| 真实并发 | advisory gate 与 `pg_stat_activity` 锁等待固定写入→推进顺序；独立 observer 看不到中间状态，推进计入先提交请求，旧重试随后过期 |
| 进程故障 | writer 已暂存行且等待写回执时实际 SIGKILL，服务端回滚后显式重试；另实际 SIGKILL PG／MinIO，重开先精确匹配状态，再接管和写入 |

满配额 fixture 用一次 COPY 登记 **99,984** 条合法形状、对应无效果 delete 的回执，使 catalog 总数达到 100,000；没有声称执行了 100,000 次 API 写入或证明持续吞吐。接近 uint64 末端的 fixture 也显式 seed 当前 epoch，再实际推进与写入；不把不可行的海量循环作为已执行证据。大 BLOB 的 oracle 统一比较 NULL 标记、长度和 SHA，避免把其重复展开为大份十六进制日志。

## 复现与冻结结果

```bash
/path/to/isolated/python -I test/vane/test_vane_vortex_postgres_metadata.py \
  --cpus 6,7,8,9,10,11 --pg-bin /path/to/postgresql-15/bin \
  --minio /path/to/pinned/minio --object-data-dir /fresh/objects \
  --output-dir /fresh/typed-proof
```

依赖继续使用 [#92 requirements](../test/vane/requirements-remote-backend.txt)、PostgreSQL 15.13 与固定 MinIO SHA；对象 fixture 显式使用新的 `/dev/shm` 目录，结束后只删除自己创建的目录。[CI](../.github/workflows/VortexRemoteBackend.yml)在模型／backend conformance 后增加 typed 检查并留存证据，不运行稳定负载测试。Hosted runner 选择其可用 CPU，实际版本与 affinity 以报告为准。

冻结专项通过 **121 项检查，其中 51 项预期拒绝，9 组故障／并发观测**；包含 writer、PostgreSQL、MinIO **3 次实际 SIGKILL（-9）**。主报告 `/tmp/ducklake-vortex-typed-proof-20261107/report.json`，SHA-256 **`50c6eac12d07d9dca55b0862980241076e2b993a2ca3773cea05863cd7dbea7d`**，记录原始检查结果、协议帧观测、容量 seed 数、物理 PG 类型、最终 typed 状态、源码身份及 CPU／线程环境；不保存 S3 secret 或签名票据。

#92 回归通过 **458 项接口、9 项能力检查、7 组故障／并发观测**，报告 SHA 为 `94a3aa076096d0d61bd806ec92a7efa13eb245f179794f352972ee0451dcfc0a`。#91 模型保持 **39 场景、18 个不安全变体反例、905＋437 个状态**通过，状态集合摘要不变；报告 SHA 为 `16cd45908a9eafd81819a7d7af93145a49648e49eaba533052cf3498033d0059`。

Vane 原基础 launcher 分独立非 Ray／Ray 进程，在 CPU 6–11 通过 **1,222＋32＝1,254 项**，可选依赖缺失跳过 **11＋4＝15 项**；日志为 `/tmp/ducklake-vortex-typed-base-20261107.log`。格式、文档链接、workflow YAML／shell、copyleft、diff 检查通过。专项目录的 `checkout-audit.json` 核对执行源码、#92 原生源码／pins／adapter／依赖，以及与 #92 相同的三个已安装 native／provider artifact SHA。本轮未修改 Vane 包源，也未重建或重装 provider。

下一项推荐先把 typed `table_uuid`／`table_id`／schema／snapshot 与 source、generation、publication、ReadView 统一绑定，验证跨 UUID 隔离与退休引用释放；随后完成 reader session fencing／pin 回收和原生调用接线，再讨论解除 native 限制。真实 AWS／TLS／IAM、HA／掉电、分布式 fan-out／top-k 和 R7 仍未完成。
