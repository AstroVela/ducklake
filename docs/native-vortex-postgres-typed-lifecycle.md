# R6 PostgreSQL 类型化元数据与 UUID／epoch 生命周期契约

本轮以 #92 合入后的开发提交 `9c3db00b` 为基线。独立的 typed metadata 组件复用 PostgreSQL／S3 broker 的 owner 和事务边界，在真实 PostgreSQL 上验证固定布局与生命周期。[组件实现](../scripts/vortex_postgres_metadata.py) · [发布 broker](native-vortex-postgres-s3-backend.md) · [总设计与 roadmap](ducklake-vortex-design-roadmap.md)

**本轮资格限于 broker 组件层。** 原生 C++／Rust、现有 DuckLake inline codec、native DuckDB 限制、SQL 接口、provider 和 #92 发布 adapter 均不修改。Vane 远程 SQL／ANN、实际 source／generation 与 typed UUID 的统一绑定仍未接入。新组件不读旧 typed 布局、不迁移、不自动转换 UUID／epoch，也不新增兼容路径或 fallback。

本地保持 CPU **6–11**、原线程环境、原生 `threads=2`、既有事务保护和预算。原生 maintenance 的单 worker／全局预算保持。R4 A/A、性能 A/B、长期稳态和依赖稳定负载的 R5 资格继续暂缓；时间只作为观察值。

## 原生路径审计与本轮范围

[PostgresMetadataManager](../src/metadata_manager/postgres_metadata_manager.cpp) 已把 FLOAT 映射为 REAL、DOUBLE 映射为 DOUBLE PRECISION，VARCHAR／BLOB 使用 BYTEA；LIST 被标为非 native 类型。[通用 metadata manager](../src/storage/ducklake_metadata_manager.cpp) 的 `GetColumnType` 会把非 native 的嵌套类型表示为 VARCHAR。因此，新的 REAL[] 组件验证**不等于现有 C++ LIST 文本 codec 已通过远程资格**，后续接线必须明确处理边界。

PostgreSQL BIGINT 是有符号 64 位，无法表示完整的正 uint64 epoch；NUMERIC 可精确保存该范围。PG 数组的声明不自动强制维度及长度，TEXT 不能存 NUL，因此需要显式的数组约束与字节表示。[PG 数值类型](https://www.postgresql.org/docs/15/datatype-numeric.html)、[PG 数组](https://www.postgresql.org/docs/15/arrays.html)、[PG 字符类型](https://www.postgresql.org/docs/15/datatype-character.html)

本轮固定一个新 typed-v1 布局，用参数化 psycopg SQL 访问真实 PG 类型；不为不同 PG／DuckDB 编码提供选择分支。测试 schema 增加可空标量、BLOB 和通用列表探针；它不是把原生四业务列查询扩展为通用 schema 支持。

## 固定类型与身份

| 逻辑值 | PG 表示与约束 | 验证 |
| --- | --- | --- |
| 表 UUID | UUID，历史 UUID 不复用，活跃名称唯一 | 同名重建后，旧 UUID 写入、推进和退休均拒绝。只允许最后一个同名实例重放原退休结果 |
| 字段身份 | BIGINT field ID、ordinal、逻辑类型和 nullability 进入 schema SHA | 布局变化时旧摘要被拒绝。同布局重建保留摘要，表实例由 UUID 区分 |
| 请求 epoch | NUMERIC(20,0)，CHECK `1..18446744073709551615`；API 仅接受整数 | 跨 `2^63`、uint64 最大值、最后一次推进重试与耗尽拒绝 |
| 行键／物理行地址 | BIGINT；物理 row ID 单调分配，UUID＋快照限定版本 | 最小／最大 BIGINT、替换／删除、历史视图与退休后不复用 |
| 有限向量 | REAL[]，显式一维、下界 1、维度匹配、非 NULL 元素、无 NaN／Inf | 1／2／3／4096 维、signed zero、最小次正规数、最大有限值、非法数组拒绝 |
| 可空 FLOAT／DOUBLE | REAL／DOUBLE PRECISION | OID 与 `float4send`／`float8send` 字节；NULL、signed zero、次正规数 |
| 可空业务字符串 | 严格 UTF-8 编码的 BYTEA | NUL、中文／重音／引号、空串与 NULL；拒绝无效 Unicode |
| BLOB／可空通用 LIST | BYTEA／REAL[] | 原始二进制、NULL／空字节，NULL／空列表／含 NULL 元素列表分别保持 |

Schema hash 包含 field ID、ordinal、名称、逻辑类型、可空性和向量维度。请求 hash 使用带长度和 NULL 标记的单一二进制格式，按业务键排序。Float32 和 Double 使用网络序 IEEE 字。传给 Float32 字段的整数按原值舍入一次，正好位于中点时取偶数。向量、可空 FLOAT 和列表使用同一规则。NULL、空值和 signed zero 各自保留。

固定 fixture 的 schema SHA 为 `275033cd1b8b6867acf4d8c0e3825a8fa1cb245806626ee2341a8d693e4fb6cc`，请求 SHA 为 `7cc8a1ee2223fa3b810cf8a0842976765d03582accf02d4ecea5919479ecc0ef`；同时要求 IEEE golden words `3dcccccd`、`80000000`、`00000001`、`3fb999999999999a`。它们约束协议本身，防止两边实现同时漂移后仍互相“验证通过”。

## 事务、窗口与退休

新增五张独立表：`typed_catalog` 保存布局／快照和分配计数；`typed_tables` 保存 UUID、名称、schema／维度、生命周期与当前窗口；`typed_fields` 保存不可变字段身份；`typed_rows` 保存 typed 行及 begin／end snapshot；`typed_receipts` 保存三元请求身份及原始回执。初始化必须创建新布局，没有 schema migration。

初始化、创建、写入、推进和退休均要求正整数 owner 令牌。令牌有效后，写操作复用 #92 的 `control … FOR UPDATE`，在同一事务内检查 owner fence、store binding 及操作涉及的 UUID、窗口和 schema hash。写批次把旧行版本关闭、新 typed 行、物理 row ID、快照分配和请求回执一起提交。没有实际数据效果的 delete 仍记录操作数与回执，并沿用现有 snapshot。

请求身份为 `(table_uuid, request_epoch, request_id)`。当前窗口的相同 typed 内容返回原回执，改变内容失败；重试不复活已经被后续操作删除的行。推进只清除目标 UUID 的当前窗口回执并原子保存下一窗口和最近一次推进结果，不改业务行、物理版本或其他 UUID。最近一次推进重试返回原结果，保留新窗口已提交回执；更早的推进重试失败。旧 epoch，包括从未提交过的 ID，均拒绝；future／零／非整数／越界 epoch 也拒绝。最大 uint64 不回绕。

退休在一个事务内结束目标 UUID 的活跃行版本，释放其 managed 标记、epoch、最近推进状态和回执。表、字段和行的历史继续服务确切 snapshot 读取。退休回放按名称查找最后一个登记的实例，只有该 UUID 已退休时才返回原退休快照。同名新实例从 epoch 1 可使用同一 request ID，即使字段布局相同也使用新 UUID。真实旧 PostgreSQL REPEATABLE READ 事务仍能读其原 tuple snapshot，显式 `read_at(old_uuid,snapshot)` 继续读取保留历史。

这些动作只操作新 typed registry；#92 的 publication fixture UUID／epoch 和 source／generation 引用保持原状，不能据此宣称 typed 退休已经释放真实索引引用。历史物理清理、reader session 回收和端到端原生退休仍需后续统一身份接线。本轮不建立独立永久 tombstone 或总磁盘／WAL／RSS 上界。

## 预算与独立验收

保持每批 **1–1,000 操作／4 MiB**、catalog 合计 **100,000 回执**；当前 UUID 的精确重试在满额时仍可返回原结果。新 typed 组件另按既有 envelope 检查 100,000 活跃行、64 MiB 向量与维度 1–4096。原生 T/F、source／artifact／manifest、三 B、GC 16 对象／64 MiB 等预算未改变；该组件尚未替代原生 admission 或 optimizer。

[独立 oracle](../test/vane/vortex_postgres_metadata_oracle.py)不调用组件的规范化、编码、hash、窗口或退休函数：直接读取五张 PG 表、列 OID、服务端 IEEE 字节与有序回执集合 SHA，对照单独的版本历史模型。[Coordinator](../test/vane/test_vane_vortex_postgres_metadata.py)每次也核对 #92 七张表和所有 S3 版本／实际字节，要求原发布、命名／native／reader 引用和对象保持；只有显式 owner 接管允许改变原 owner 一项。

| 验收组 | 固定条件 |
| --- | --- |
| 类型／拒绝 | 真实 PG OID、IEEE golden、binary cursor 回读、UTF-8／NUL／BLOB、NULL／空值、字段绑定和直接 SQL CHECK 拒绝。Float32 整数用固定字节验证中点、溢出及同 ID 重试 |
| Owner 与 UUID | 各写入口校验令牌，接管后拒绝旧 owner。同名后继实例活跃及退休后均拒绝旧 UUID 的退休回放。同布局重建通过 UUID 区分实例 |
| 窗口／容量 | 按原门槛验证 4 MiB 批次和 100,000 回执。独立验证 100,000 活跃行与 64 MiB 向量，恰满可提交，超额回滚，删除一行后可再次提交 |
| 不确定 COMMIT | mutation／advance／retire 分别覆盖 COMMIT 转发前断开、服务端确认后丢 ACK，必须全旧或全新；随后按原 UUID／epoch 显式重试 |
| 事务内错误 | 三类操作分别在行暂存、回执清理、退休状态更新边界由测试 trigger 抛错，恢复全部 typed 表、计数与原对象状态 |
| 真实并发 | advisory gate 与 `pg_stat_activity` 锁等待固定写入→推进顺序；独立 observer 看不到中间状态，推进计入先提交请求，旧重试随后过期 |
| 进程故障 | writer 已暂存行且等待写回执时实际 SIGKILL，服务端回滚后显式重试；另实际 SIGKILL PG／MinIO，重开先精确匹配状态，再接管和写入 |

回执配额和接近 uint64 末端的用例使用固定种子，再走真实 API 验证边界。活跃容量分别在隔离 schema 中种入 99,999 条一维行和 4,095 条 4,096 维行。超额用例同时替换一行并新增一行，由独立读取核对五张 typed 表和全部物理行版本摘要。大 BLOB 的 oracle 比较 NULL 标记、长度和 SHA。

## 复现与冻结结果

```bash
/path/to/isolated/python -I test/vane/test_vane_vortex_postgres_metadata.py \
  --cpus 6,7,8,9,10,11 --pg-bin /path/to/postgresql-15/bin \
  --minio /path/to/pinned/minio --object-data-dir /fresh/objects \
  --output-dir /fresh/typed-proof
```

依赖继续使用 [#92 requirements](../test/vane/requirements-remote-backend.txt)、PostgreSQL 15.13 与固定 MinIO SHA；对象 fixture 显式使用新的 `/dev/shm` 目录，结束后只删除自己创建的目录。[CI](../.github/workflows/VortexRemoteBackend.yml)在模型／backend conformance 后增加 typed 检查并留存证据，不运行稳定负载测试。Hosted runner 选择其可用 CPU，实际版本与 affinity 以报告为准。

当前开发基线上的 PostgreSQL 专项通过 **169 项检查，其中 85 项预期拒绝，9 组故障或并发观测**。Writer、PostgreSQL 和 MinIO 实际退出码均为 `-9`。报告记录执行源码摘要、物理 PG 类型、容量、协议帧和最终状态。

同期 #92 后端通过 485 项接口检查、9 项能力检查和 7 组故障或并发观测。#91 模型通过 40 个场景、21 个负对照和 1,342 个有界状态。

2026-11-07 的原 PR 基于 #92 的旧叠栈提交 `44b3c16e`。当时 typed 专项通过 121 项检查，其中 51 项预期拒绝和 9 组故障或并发观测。历史报告路径为 `/tmp/ducklake-vortex-typed-proof-20261107/report.json`，SHA-256 为 `50c6eac12d07d9dca55b0862980241076e2b993a2ca3773cea05863cd7dbea7d`。

原 PR 同轮的 #92 回归通过 458 项接口、9 项能力检查和 7 组故障或并发观测，报告 SHA-256 为 `94a3aa076096d0d61bd806ec92a7efa13eb245f179794f352972ee0451dcfc0a`。#91 模型通过 39 场景、18 个负对照和 905＋437 个状态，历史报告 SHA-256 为 `16cd45908a9eafd81819a7d7af93145a49648e49eaba533052cf3498033d0059`。

原 PR 的 Vane 基础 launcher 在 CPU 6–11 分独立非 Ray 和 Ray 进程运行，共通过 1,254 项，跳过 15 项。历史日志路径为 `/tmp/ducklake-vortex-typed-base-20261107.log`。

下一项推荐先把 typed `table_uuid`／`table_id`／schema／snapshot 与 source、generation、publication、ReadView 统一绑定，验证跨 UUID 隔离与退休引用释放；随后完成 reader session fencing／pin 回收和原生调用接线，再讨论解除 native 限制。真实 AWS／TLS／IAM、HA／掉电、分布式 fan-out／top-k 和 R7 仍未完成。
