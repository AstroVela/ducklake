# DuckLake＋Vortex 总设计与 roadmap

本文是 DuckLake＋Vortex 实时向量检索的统一设计入口，按 **当前实现与验证结果**整理。分项 proof 文档保留各次实验的历史；本文说明当前采用的方案、能力边界和后续顺序。

配套阅读：[ChatGPT Page](https://chatgpt.com/space/page_0c2fa263e4dc81919bfaea100e8008cd)。本文是后续随代码维护的仓库总纲。

**当前结论：本地原生基本链路已实现并通过端到端验证；持续写入下的长期维护稳态尚未完成。** 已支持提交后即时查询、后台 flush、独立 B 段构建、选择性合并、数据重写、受事务保护的 GC 和崩溃恢复。当前没有接入 SPFresh 原生在线增删改，也没有完成云端或分布式向量查询。

## 1. 基线与设计范围

实现基线为 [DuckLake Draft #74](https://github.com/AstroVela/ducklake/pull/74)，head 为 `2a9377062cddc0c222d8dd99e4d5efee3a298c76`。#74 的受控 A/B 未达到预设收益门槛，已撤回生产查询合并；其生产 `src/` 与 [#73](https://github.com/AstroVela/ducklake/pull/73) 的 `9819016e841ea79c8eea054415f4724fa2270e85` 相同。本文的“已验证”指该实现闭包的本地验证，不代表这些 Draft 已合入、发布或通过全部远端 CI。

上述架构基线之后，[#84](https://github.com/AstroVela/ducklake/pull/84) 增加[数值修复与确定性功能验收](native-vortex-functional-matrix.md)：inline FLOAT／DOUBLE 使用显式浮点类型解析，B/F/T 的平方 L2 使用双精度累加后返回 Float32。[#85](https://github.com/AstroVela/ducklake/pull/85) 增加[显式请求 epoch 与回执清理](native-vortex-request-epochs.md)，在同一原生事务中推进窗口和删除旧回执。[#86](https://github.com/AstroVela/ducklake/pull/86) 补充 [reference／WAL 生命周期契约](native-vortex-reference-wal-lifecycle.md)；[#87](https://github.com/AstroVela/ducklake/pull/87) 验证[容量 admission 边界](native-vortex-capacity-admission.md)，该轮生产 `src/` 与 #85 的 `f67dd3bfe31278bf425435e9a3822d07d04b9774` 相同。[#88](https://github.com/AstroVela/ducklake/pull/88) 新增[查询 scratch 所有权与显式回收](native-vortex-query-scratch-lifecycle.md)，为私有副本建立登记、活跃文件锁及有预算的崩溃回收。[#89](https://github.com/AstroVela/ducklake/pull/89) 进一步实现[表 UUID 状态生命周期](native-vortex-table-uuid-lifecycle.md)：请求显式绑定 UUID，表 DROP 与该 UUID 的状态释放原子提交。[#90](https://github.com/AstroVela/ducklake/pull/90) 补充[已退休表的原生历史与物理回收契约](native-vortex-retired-history.md)，修复过期后遗留 table-scoped 配置的问题。[#91](https://github.com/AstroVela/ducklake/pull/91) 提供 [R6 远程发布协议与独立模型](native-vortex-remote-publication.md)。[#92](https://github.com/AstroVela/ducklake/pull/92) 增加 [PostgreSQL／S3 backend 与真实服务验证](native-vortex-postgres-s3-backend.md)，[#93](https://github.com/AstroVela/ducklake/pull/93) 提供 [PostgreSQL typed metadata 与 UUID／epoch 生命周期契约](native-vortex-postgres-typed-lifecycle.md)。本轮进一步实现 [typed UUID、发布与 ReadView 统一绑定](native-vortex-bound-readviews.md)，采用新 storage／typed v2 布局并连接退休引用；原生 C++／Rust、provider 和 native 限制保持。#83 的文件名候选仍未采用，既有预算和事务保护保持。这些验证性 Draft 均不代表已合入或发布。

当前限定：单进程独占 native DuckDB 元数据目录、本地受管 POSIX 文件、无分区／未加密的固定业务 schema、Float32 有限向量、平方 L2 距离。并发客户端与后台维护位于这一所有权范围内；多进程写者、跨主机 fencing、远程对象存储和分布式 ANN 不在当前验证范围。

```sql
CREATE TABLE lake.items (
    id BIGINT,
    embedding FLOAT[],
    category VARCHAR,
    payload VARCHAR
);
```

受管写入要求非 NULL `id`；`category`、`payload` 可以为 NULL。当前不是任意 schema、距离度量或 SQL 谓词的通用向量优化器。

本设计继承[原 Iceberg＋Vortex RFC](https://chatgpt.com/space/page_5ee0f8c698d08191a17198d45d152362)中“可查询增量＋不可变文件＋异步索引”的方向，以及参考 Qdrant 的 segment 生命周期和后台 optimizer 分工。实际落地改用 DuckLake 原生事务、inline 数据、删除可见性和元数据 WAL；原 RFC 的独立 WAL、HotVersions、Iceberg 物化与 ServingManifest 不能据此标为已实现。

## 2. 总体架构与职责

| 组件 | 当前职责 |
| --- | --- |
| DuckLake | 业务行与删除版本、事务快照、inline 数据、文件清单、写入回执、索引与维护元数据 |
| Vortex 文件 | 保存落盘后的向量和业务列；保留可验证的物理文件身份 |
| vortex-index／SPFresh | 从不可变物理输入构建、封存和查询静态 ANN generation |
| ReadView | 固定一次查询的原生快照、B/F/T 分区、可见性、过滤条件及资源所有权 |
| optimizer／maintenance | 阈值触发 flush、建索引、合并、重写，以及受预算限制的恢复与回收 |
| Vane 原生 provider | 通过安装后的 `vane.connect()` 暴露 SQL 入口；C++ 经 Rust ABI 调用 Vortex 查询核心 |

```mermaid
flowchart TD
    W[受管 upsert / delete 批次] --> D[DuckLake 原子提交：行、删除版本、回执]
    D --> T[T：已提交 inline 向量]
    T -->|后台 flush| F[F：尚无 ANN 的 Vortex 文件]
    F -->|独立静态构建与原子发布| B[B1、B2、B3：已索引不可变段]
    D --> R[捕获一个 committed ReadView]
    T --> R
    F --> R
    B --> R
    R --> Q[按同一可见性和 category 条件检索]
    Q --> A[B：ANN；F/T：精确距离]
    A --> K[统一排序、全局 top-k、同视图取业务列]
    B --> C[选择性合并 / 数据重写 / 退休]
    C --> G[事务与所有权保护下的 GC]
```

T/F/B 是**同一份逻辑数据在某个 ReadView 下的物理分区**，不要求客户端切换查询 API，也不要求最新写入等待 ANN 建成。F/T 的精确计算是预定查询路径；ANN 出错或结果不足不会触发扫描重试。

## 3. 数据状态与身份

| 状态 | 含义 | 查询方式 | 后台变化 |
| --- | --- | --- | --- |
| T | 已提交、尚未 flush 的 inline 数据 | 原始向量精确计算 | 达到策略阈值后 flush 到 F |
| F | 已落 Vortex 文件、尚未被当前 B 覆盖 | 原始向量精确计算 | 达到阈值且至少 65 个物理行后构建 B |
| B | 被已发布静态 generation 覆盖的文件集合 | 对可见且匹配的行执行 ANN，候选重新计算距离 | 选择性合并、数据重写、退休 |

命名索引允许 **零 B**。空表、只有 T、只有小 F、首次构建中和多 B/F/T 都使用同一个命名查询。小尾部可以继续留在 T/F；不会为了建 ANN 填充数据或放宽 65 行下限。

需要区分四种身份：

- 业务 `id` 表示逻辑键；受管写入中每个键只有一个当前版本。
- DuckLake `snapshot_id` 决定查询可见的逻辑版本，也包括只改变物理布局的快照。
- 文件 ID、URI、内容摘要、物理行数及行地址标识实际索引输入；绑定还校验表 UUID、向量字段 ID、类型、维度和距离度量。
- 索引 catalog revision 与 generation 标识索引发布状态；它们不等于业务快照号，也不使索引自动拥有最新删除可见性。

当前 catalog 使用严格的 manifest-v3 表示，支持零 B；格式或绑定不匹配直接失败，不提供旧格式兼容解码。详见[基本链路](native-vortex-basic-flow.md)、[多 B 段](native-vortex-segments-proof.md)及[原生 ReadView 实现](../src/storage/ducklake_vortex_read_view.cpp)。

## 4. 写入、ACK 与幂等

推荐业务入口是显式注册后的受管 mutation API：

1. 初始化索引、optimizer 和写入 catalog，在空表建立带维度的零 B 定义。
2. 调用 `ducklake_manage_vortex_writes` 注册空表所有权；配置写入预算与后台策略。
3. 创建请求时保存 `table_uuid` 和当前正整数 `request_epoch`；调用 `ducklake_mutate_vortex_batch`，提交请求 ID、完整 upsert 行、待删除 ID、该 epoch 及 UUID。所有重试保留原 UUID 和 epoch。
4. 在同一原生事务内删除旧版本、插入新 T 行并登记回执；成功提交后返回 ACK。

upsert 是整行替换。一个批次内 upsert/delete 的 ID 必须非 NULL 且互不重复。同一个 ID 同时出现在两类操作中会失败。回执包含 `table_uuid, request_id, payload_hash, row_count, snapshot_id, request_epoch`；`row_count` 是请求操作数，删除不存在的键也计入。没有实际数据变化的删除可沿用当前快照号，但回执仍持久提交。

幂等身份为 `(table_uuid, request_epoch, request_id)`。新表从 epoch 1 开始；当前窗口内相同内容重试返回原回执，更改内容复用请求 ID 会失败。重放旧 upsert 不会复活后来删除的数据，重放旧 delete 不会删除后来新写入的版本。提交冲突向客户端返回，内部提交重试次数为零；客户端可在新事务中显式重提同一身份。过期 epoch 一律返回 `EXPIRED_REQUEST`，包括从未提交的请求 ID；未来 epoch 也拒绝。不能在重试时读取最新 UUID／epoch 并改写旧请求身份。同名重建后的新表使用新 UUID；旧 UUID 的写入与窗口推进拒绝执行，不能变成新表请求。

受管表拒绝普通 SQL INSERT/UPDATE/DELETE、append API，以及 schema、rename、drop 变更；flush、重写与回收通过已有内部授权路径执行；表退休使用带确切 UUID 校验的 `ducklake_retire_vortex_table`。非受管表保留普通 SQL／append 语义。这里复用 DuckLake 原生 MVCC 和元数据 WAL，没有第二份写入 WAL 或独立的逐键版本目录。

回执记录上限仍为**每个元数据 catalog 合计 100,000 条**，没有自动过期。`ducklake_vortex_request_status` 返回当前窗口及表／catalog 计数；显式 `ducklake_advance_vortex_request_epoch` 与写入共用串行化锁，在同一原生元数据事务内核对 expected UUID／epoch、删除该表旧窗口回执并持久推进 epoch。最近一次推进可返回原结果，重试不会删除新窗口的回执。推进意味着接受当前及尚未提交的旧请求失去重试窗口；达到 uint64 上限则拒绝推进。

快照过期和源文件 GC 不会删除回执。显式推进释放逻辑回执槽位，不承诺立即缩减元数据文件、WAL 或 RSS；显式表退休在同一 DROP 提交中释放该 UUID 的 epoch、回执、managed 注册、预算、policy 和命名引用；最后一个引用解除后 generation 标记 retired。原生表／inline 历史和元数据文件仍有独立物理生命周期。新布局为 requests-v2／epochs-v1，保留原 managed 注册表；旧写入 SQL 参数个数和旧回执布局直接拒绝，不迁移、不加兼容模式。详见[当前 UUID 接口与退休契约](native-vortex-table-uuid-lifecycle.md)、[回执生命周期](native-vortex-request-epochs.md)、[受管写入](native-vortex-keyed-writes.md)和[写入实现](../src/storage/ducklake_vortex_write.cpp)。

## 5. 一次查询如何覆盖 B/F/T

查询捕获一个独立的 committed ReadView；调用方尚未提交的写入不会混入其中。该视图一次固定源文件、已提交 T 行、原生删除可见性、业务过滤条件、B generation 和资源所有权。

执行顺序：

1. 校验当前索引定义与物理输入绑定，把源数据划分为互不重叠的 B、F、T。
2. 在各分支 top-k **之前**应用该快照的删除可见性和业务过滤条件。
3. 每个 B 查询静态 ANN；F/T 对原始向量计算平方 L2 距离。
4. 用所持有的向量重新计算 B 候选距离，与 F/T 合成一个全局 top-k，按稳定物理行顺序打破距离相同的平局。
5. 从同一个 ReadView 取回类型化业务列，避免向量来自旧版本而 payload 来自新版本。

例如，B 中旧 `id=7` 被 upsert 后，旧物理行先被可见性过滤，新 T 行参与检索；不用末尾按业务 ID 去重来掩盖过期命中。该机制保证版本与过滤正确性，ANN 召回率仍需要独立测量，结果可以少于 k。

```sql
SELECT id, embedding, category, payload, distance
FROM ducklake_vortex_search(
    'lake', 'main', 'items', 'knn', [0,0]::FLOAT[], 10,
    category => 'A');
```

当前只有命名参数 `category` 的区分大小写等值条件是原生业务预过滤；外层 SQL `WHERE` 只过滤已返回命中，不等价于任意谓词下推后的 top-k。

`ducklake_vortex_search_diagnostics` 使用同一执行结果，固定返回一行 `diagnostics` 和类型化 `hits` 列表；即使零命中也能记录实际 `native_capture.snapshot_id`。普通 search 仍返回上面的五个业务／距离列。验证 oracle 使用**这次调用实际捕获的快照**，不会从前后提交水位中选择更有利的版本。

已捕获视图持有源字节、T 行和私有 generation lease。规范路径上的文件或索引经过 GC 后，旧视图仍可查询和延迟取列；新视图必须重新验证当前文件。当前会持有／复制受限输入，尚未完成大规模完全 out-of-core ANN 查询。详见[快照与维护观测](native-vortex-readview-maintenance-profile.md)及[SQL 查询入口](../src/functions/ducklake_vortex_search_function.cpp)。

## 6. 后台索引与物理维护

### 6.1 独立构建与原子发布

普通追加只把新 F 构建为 B₂，保留 B₁ 的 generation、描述符、产物内容和修改时间。B₁ 的身份校验仍有读取成本；“不重建旧 B”不表示发布过程完全不访问旧 B。

重型构建在元数据事务和发布锁之外执行。候选 generation 经过封存、输入／产物校验，再以 expected revision CAS 原子发布完整段集合。失败、取消或并发发布冲突不会暴露半套索引。发布成功但响应丢失时，恢复检查持久发布身份，不重复构建已经提交的 generation。

### 6.2 三段预算下的选择性合并

当前最多三个 B，所有段共享物理行、源字节、向量字节与产物预算。新 F 达到建索引条件但段槽已满时，optimizer 先选择两个最小 B 做独立合并任务；未选中的 B 保持不变。后续任务重新检查输入，再追加 F。

索引合并保留源文件与物理行地址，因此**不会释放已删除版本占用的源物理容量**。显式全量重建仍可由操作员调用；普通增量路径不依赖每次重建全部 B。

### 6.3 数据重写、稀疏段与退休

启用数据 compaction 策略后，按已有阈值选择可回收候选：

| 选中 B 的剩余可见行 | 计划 |
| --- | --- |
| 0 | 退休该 B；源文件继续遵守原生快照保留规则 |
| 1–64 | 显式重写为 F，不构建 SPFresh generation |
| 至少 65 | 重写可见源数据并构建替代 B |

整文件已不可见时还可走已验证的文件裁剪路径。原生文件清单和索引引用在同一发布事务更新，未选中 B 不变。`rewrite_f` 在规划时显式选择，不由 ANN 失败触发。

物理重写从计划选择、持久任务登记、源捕获到发布持有现有受管写者 reservation，避免后续 mutation 使选中非空输入变空。该范围会影响前台 ACK 等待；独立 B 构建和索引合并不持有这份受管写者 reservation。当前没有生产写延迟保证。

详见[选择性合并](native-vortex-segment-merge-proof.md)、[索引与数据重写](native-vortex-index-rewrite-proof.md)、[稀疏 B→F](native-vortex-sparse-rewrite-proof.md)、[文件裁剪](native-vortex-prune-files-proof.md)及[规划串行化](native-vortex-rewrite-planning.md)。

## 7. GC、事务保护与崩溃恢复

### 7.1 三类回收与共享预算

maintenance 复用现有单 worker，对 build 恢复／回收、退休 generation、源快照／文件回收共享每轮对象与字节预算，并轮换首先服务的类别。预算计算完成的回收对象／字节，不是所有校验读取、WAL 写入、线程数或进程 RSS 的上限。文件大于剩余字节预算时继续待处理，不绕过预算删除。

源保留策略默认关闭，显式开启后是 **catalog 范围**，会影响同 catalog 的非向量 DuckLake 表。快照保留数量与最小年龄必须按业务设置；测试中的保留两个快照、年龄零不属于生产建议。

### 7.2 删除前重新检查

源 collector 按以下顺序关闭入口：**publication exclusive → ReadView capture admission exclusive → transaction-start gate exclusive → 普通事务检查**。readiness 探测不能代替删除前的独立检查；仍有普通事务就暂缓，不穿透长事务删除文件。

只回收原生删除队列和持久删除意图登记的受管文件，不扫描目录后猜测 orphan。删除前校验受管根目录、路径／文件类型、引用、文件身份、大小与 SHA-256，拒绝符号链接等不满足所有权条件的对象。意图先提交，再 unlink 并同步父目录，最后原子确认队列、意图和统计。仅已登记意图允许恢复时处理“文件已不存在”。

### 7.3 事务结束后的唤醒

#73 在观察到阻塞事务时原子登记一次性回调。观察到的事务首次结束后，在事务管理器锁外通知 worker；通知只请求重新探测，不授权 GC。新的／剩余事务、引用和文件身份仍需重新检查。

回调绑定弱 runtime 引用和 maintenance profile 身份，取消或被新 registration 取代后忽略。wake epoch 防止通知发生在探测结束与进入等待之间而丢失；既有 100 ms idle wait 和预算保持不变。

### 7.4 持久状态与恢复边界

原生元数据保存命名索引、generation／引用、build／rewrite 所有权、optimizer／maintenance 策略、写预算、受管表、请求回执及 epoch、源保留策略和删除意图。候选产物与删除意图遵守持久登记协议，不把任意残留目录当作可接管工作。

提交前失败回滚，提交后丢失 ACK 通过回执／发布身份核对。原生元数据 WAL 恢复已提交状态；进程重启本身不启动后台工作，需要显式 `ducklake_resume_vortex_optimizer` 和 `ducklake_resume_vortex_maintenance`。错误或取消停止对应调度；resume 重新检查持久状态和输入，不静默重放失败任务。

详见[源保留与删除协议](native-vortex-source-retention.md)、[事务完成唤醒](native-vortex-transaction-gc-wakeup.md)、[自动全链路与 SIGKILL](native-vortex-automatic-flow.md)。早期 proof 中被后续更改的锁顺序、段数或 manifest 版本，以本基线代码及后续文档为准。

### 7.5 Reference 与元数据 WAL 的生命周期

调用方 reference JSON、catalog 中的命名引用和打开的 ReadView 是三类不同所有权。发布会把 descriptor 复制到原生元数据；删除调用方 JSON 不会释放命名引用，保留 JSON 也不会阻止无引用 generation 的 GC。共享 B 必须等所有命名引用解除后才能回收；已完全打开的 ReadView 使用私有副本，仍可在规范 generation 被删除及 metadata checkpoint 后完成查询。本轮查询先登记私有命名空间和 owner，再创建 SDK 副本；正常结束释放登记和 payload，空协议根保留。SIGKILL 后重开仍不自动删除副本；调用方可以用诊断中的 `scratch_root` 显式执行有预算回收，活跃文件锁和持久删除意图保护旧视图及部分清理。未登记目录不被扫描接管，受管 source／generation GC、调用方 reference 和原生 WAL 保持独立。详见[查询 scratch 生命周期](native-vortex-query-scratch-lifecycle.md)。

原生元数据 WAL 由 DuckDB 管理，显式对该原生元数据库执行非强制 checkpoint。`CHECKPOINT lake` 会执行 DuckLake 的 flush、过期和文件维护，不能代替这个操作。活跃写者或仍保留已提交 UPDATE 的旧读者可能阻止 checkpoint；原事务保护保持，读者结束后由调用方显式重试。无并发写者时 checkpoint 可移除旧 WAL，后续提交又会创建 WAL；这不等于数据文件缩小或总磁盘有界。详见[所有权与恢复契约](native-vortex-reference-wal-lifecycle.md)。

### 7.6 表 UUID 的原子退休

`ducklake_retire_vortex_table(catalog,schema,table,expected_uuid)` 在 autocommit 下校验确切 UUID，再将 DROP 与 UUID 状态释放置于同一原生事务。普通受管 DROP 的保护保持；enabled 或仍在执行的 optimizer、未清理的 build／rewrite 会阻止退休。原生提交成功后才释放内存调度状态，取消或冲突不自动重试。

调用方应在需要释放 Vortex 状态时显式退休。该入口不接管旧孤儿记录，不删除 caller JSON、源文件或 WAL；同名新表的旧 UUID 请求会失败。普通 SQL 旧读者与已打开的私有视图保留各自保护，physical GC 继续使用现有 gate 和预算。详见[表 UUID 生命周期](native-vortex-table-uuid-lifecycle.md)。

### 7.7 已退休表的原生历史与物理回收

表退休不立即删除原生历史。catalog 级 source retention 按剩余快照判定旧 table ID 是否仍可见；失去最后历史引用后，在同一原生事务中删除表／列／统计及表级配置、DROP 实际 inline 数据／删除表，并登记源文件回收。修复前 `ducklake_metadata` 会遗留已过期 table ID 的格式配置；本轮只对已确认可清理的 table ID 删除其 table-scoped 配置，保留历史尚可见的选项、catalog／schema 配置及其他活跃表。

源文件与 generation 继续通过既有 intent、身份校验、事务保护和预算执行物理 unlink。小字节预算暂缓文件删除，仍可在剩余对象预算内过期元数据快照。原生 checkpoint 可释放和复用块，但文件尺寸仍可能增长；有限循环及 WAL 清理不构成总磁盘／RSS 或长期容量上界。详见[退休历史与物理回收](native-vortex-retired-history.md)。

## 8. 资源边界与运行入口

以下是当前原型的硬边界，不是生产容量承诺：

| 项目 | 当前边界 |
| --- | --- |
| 向量 | Float32，有限分量，维度 1–4096，平方 L2 |
| 查询 k | 1–64 |
| 新建 SPFresh B | 65–100,000 个物理行，并满足其他全局预算 |
| B 段数 | 每个命名计划最多 3 个 |
| 物理行／向量 | 全局最多 100,000 行、64 MiB 向量；维度 d 的行上限还受 `floor(64 MiB / (4*d))` 约束 |
| 编码源／索引产物 | 分别全局 64 MiB，不按 B 段数倍增 |
| catalog manifest | 1 MiB |
| mutation 批次 | 1–1,000 个操作，规范化内容最多 4 MiB |
| 请求回执 | 每个元数据 catalog 合计 100,000 条；不自动过期，显式推进单表 epoch 后清理旧窗口 |
| T/F 写入配额 | 显式配置；可见 T 按 `4*d+64+UTF8(category)+UTF8(payload)` 计费，F 按真实编码文件计费；提交前重新 admission |
| 保留 inline 历史 | 64 MiB，且与当前物理输入共享行／向量上限；包含已结束但尚未 flush 的版本 |

逻辑删除可复用可见 T 配额，但不会删除保留 inline 历史或源文件的物理行。Flush 物化已结束版本；显式重写在原子发布时从当前文件清单移除旧输入，可在旧读者仍持有源文件时复用当前 admission 槽位。源 GC 仍须等待事务与引用保护解除。详见[容量核算与证据层次](native-vortex-capacity-admission.md)。

没有扩大预算就支持无限数据增长的结论。源／索引 GC 排空也不等于总磁盘有界：回执、原生元数据／WAL、调用方 reference JSON 有独立生命周期。上述输入限额同样不构成进程 RSS 上限。

安装后入口、provider 组合及完整配置步骤见 [Vane 原生接入](vane-native-vortex-proof.md)和[自动链路示例](native-vortex-automatic-flow.md)。当前验证使用非 editable 安装、匹配的 Vane／DuckLake／Vortex providers 和显式 `VANE_RUNNER=local-fast`。本向量路径使用原生 C++／Rust 调用，不为每次查询启动 Python 子进程或进行 JSON 文件交接。普通 DuckLake 的其他 Ray 读路径不等于本设计的分布式 ANN 已接通。

当前依赖身份以 [`vane-native-vortex.toml`](../vane-native-vortex.toml)、[Rust 依赖](../src/vortex_query/Cargo.toml)及 [native CI workflow](../.github/workflows/VaneNativeVortex.yml)为准；历史 proof 的 pin 表仅对应当次验证。Vortex 索引复用 `vortex-index` 分支演进出的 `vortex-index`／`vortex-index-spfresh` 实现，不能把通用 Vortex 文件格式直接等同于已建 ANN。

## 9. 已有验证与仍未关闭的问题

### 9.1 功能与恢复

此前 #74 的安装态资格验证覆盖基本链路、append、源保留、受管写入、自动维护和停滞诊断六组，包括 38 个 native source case、13 个 maintenance case、261 个 SQL assertion block、11 个快照／计时 oracle 测试、5 个文件计量测试和 4 个 A/B 分析测试。Vane base launcher 分进程通过 1,222 个非 Ray 和 32 个 Ray 测试，另有 15 个可选依赖跳过；这些 Ray base 测试不代表分布式 ANN 资格验证。

已覆盖独立 B₂ 不重建 B₁、原子发布竞争、取消、实际 SIGKILL、提交后丢 ACK、旧请求重放、普通事务阻止 GC，以及旧 ReadView 在规范文件／generation GC 后继续查询。详见[#74 最终资格记录](native-vortex-maintenance-probe-ab.md#final-installed-qualification-after-withdrawal)。

本轮 R5 的四维度功能矩阵通过 36 个阶段、432 次普通查询、432 次实际快照诊断及 56 项拒绝检查，最低 fixture recall 1.0。另通过 inline 精确 SQL 的 11 个断言块、高维平方 L2 闭式回归、溢出拒绝及合法查询恢复；两项数值回归已在对应旧 provider 上复现失败。本轮受管／append 回归共 20 项原生／恢复用例通过，Vane 基础套件再次通过 1,254 项、15 项可选跳过。详见[确定性维度与查询契约](native-vortex-functional-matrix.md)。

[显式请求 epoch](native-vortex-request-epochs.md)通过 8 组公开契约与 14 项原生／故障／恢复用例，包括 10 万条全局回执上限、表间隔离、两种写入／推进顺序、八个并发相同推进、真实元数据冲突、取消及四个实际 SIGKILL 边界。四维度矩阵在新接口上再次通过；现有六组链路回归覆盖 67 项原生／恢复用例、172 次安装态查询比较，另通过 189 个相关 SQL 断言块与 6 项 oracle；本轮 Vane 基础套件通过 1,254 项、15 项可选跳过。回执槽位释放不代表文件／WAL 已收缩，也不构成长期容量资格。

[Reference／WAL 生命周期](native-vortex-reference-wal-lifecycle.md)进一步通过 4 个原生／重开场景和 8 个实际 SIGKILL／恢复边界，逐行比较 13 张 Vortex 元数据表、业务列与 snapshot 身份，并校验受管文件和私有副本的 SHA-256。覆盖共享 generation、调用方 JSON 删除、部分 GC 取消／失败、checkpoint 执行中及 header／WAL 完成边界、后续写入提交两侧；旧读者保留已提交 UPDATE 时 checkpoint 仍会拒绝，读者结束后才显式重试。另通过 65 项现有原生／恢复回归与 76 次 oracle 比较；Vane 基础套件通过 1,254 项、15 项可选跳过。生产 `src/` 没有改动。

[容量 admission 边界](native-vortex-capacity-admission.md)进一步通过 7 项独立模型检查、11 组真实数据边界、31 项原生 envelope 解析、7 项通用存储检查，以及 4 个原生事务／重开和 3 个实际 SIGKILL／恢复场景。真实输入覆盖 1D／167D／168D／4096D 的现有行／向量上限、4 MiB 请求与 64 MiB 保留 inline 历史。三 B 的真实 lease 按认证文件总量验证；通用大对象存储与声明解析单独标注，不作为大规模 ANN 质量资格。原子重写可复用当前物理槽位，旧读者仍阻止源 GC；推进 epoch 只释放回执，不释放数据容量。生产 `src/` 没有改动。 另通过五组链路的 77 项原生／恢复回归与 76 次已记录 fixture 比较；分进程 Vane 基础套件通过 1,254 项、15 项可选依赖跳过。

[查询 scratch 生命周期](native-vortex-query-scratch-lifecycle.md)进一步通过 25 组文件系统协议场景，以及 5 个真实查询／重开、5 个查询崩溃／重开和 7 个清理故障／重开场景，共 45 次实际 SIGKILL。验证活跃锁、64 MiB 与对象预算、唯一删除意图、inode／未知文件拒绝、部分清理恢复及轮转公平性；逐项比较 13 张元数据表、typed rows、snapshots 和全部 data／indexes 文件身份。旧视图在规范 GC／checkpoint 后仍可查询；重开不自动接管私有 payload，显式回收只作用于已认证登记。 另通过 77 项相关原生／恢复用例、76 次 fixture 比较、容量 admission 与完整四维度矩阵；分进程 Vane 基础套件通过 1,254 项、15 项可选依赖跳过。

[表 UUID 状态生命周期](native-vortex-table-uuid-lifecycle.md)进一步通过 6 组公开契约与 25 个 native／重开场景，包括 5 次实际 SIGKILL、七个阶段的取消及错误、真实原生 snapshot 主键冲突，以及八个并发相同退休。固定输入覆盖旧 UUID 不跨同名新表、另一 UUID 的真实 B／build／回执保持、100,000 回执配额释放、17 次 policy 槽位复用、普通旧读者与私有视图保护。逐项比较 13 张元数据表、原生表历史、typed rows、snapshots 和源／索引文件摘要；显式退休不执行物理 unlink。另通过 77 项既有原生／恢复回归、76 次 fixture 比较、容量与 scratch 验收，以及完整四维度矩阵；分进程 Vane 基础套件通过 1,254 项、15 项可选跳过。

[已退休表的原生历史与物理回收](native-vortex-retired-history.md)通过 4 组公开契约、33 个 native／新安装态恢复场景，含 15 次实际 SIGKILL，覆盖取消、真实 UPDATE 冲突、事务内 inline DROP 与表级配置清理的原子性，以及 source／generation 部分删除续作。逐项核对 28 张原生表、13 张 Vortex 表、实际 inline 数据／删除表和源／索引文件；三轮固定 T-only 输入直接验证 block ID 复用。旧 provider 上的新断言已复现表级配置泄漏，修复后通过。保留策略仍是 catalog 级；小字节预算不阻止有界元数据过期，块复用及 WAL 清理不代表文件缩小或总磁盘／RSS 有界。 相关六组回归通过，分进程 Vane 基础套件再次通过 1,254 项、15 项可选依赖跳过。

### 9.2 #73 基线的历史 30 分钟观察

下表是 #73 的独立本地 trace；#74 撤回查询合并后曾恢复相同生产源码。本轮 R5 的数值修复与写入 epoch 改变了相关生产路径，尚未进行新的 30 分钟验证；以下历史数据不作为当前版本的稳态资格。配置为 512 个活跃键、8D、4 个读者，maintenance 每轮 2 对象／64 MiB；T/F 配额分别为 256／1,024 行，字节配额分别为 4／64 MiB。结果仅适用于这个负载与配置。

| 指标 | 观察 |
| --- | --- |
| 实际写入阶段 | 1,800.275 秒 |
| 成功批次／mutation 操作 | 1,633／261,280，无拒绝尝试 |
| 查询 | steady 76,160；含 drain 76,595 |
| 实际 ReadView 快照／跨提交窗口查询 | 4,900／4,948 |
| Canary／exploration 最低召回 | 1.0／0.9，均通过既有 0.8 门槛 |
| 停写后 drain | 63.003 秒，通过 90 秒门槛；源快照／删除队列／意图排空 |
| 最终逻辑行／旧请求重放 | 512 行，均通过 |
| 写入结束 eligible snapshot 积压／峰值 | 613／614 |
| 写入阶段积压拟合斜率 | **+16.95 个快照／分钟** |

这证明受限混合负载下查询与恢复链路可用，停写后可以排空；**尚不能证明持续写入时积压有界**。期间 source GC 的 wall time 为 192.593 秒，其中元数据 73.707 秒、快照过期 70.551 秒，提示继续调查历史增长后的元数据／过期成本。父子计时重叠，不能相加后推断 CPU 占用或因果性能收益。完整数据见[事务唤醒验证](native-vortex-transaction-gc-wakeup.md)。

### 9.3 已撤回实验与开放故障

#74 在同一只读元数据与任务输入下做 6 个 case、3,840 次主要探测的交替配对 A/B。中／大规模四个 case 的完整读探测中位收益只有 0.17%–1.26%，未满足事先声明的收益、区间和胜出轮数要求，因此撤回 policy/backlog 查询合并；只保留测试基准。该实验没有执行实际 GC，不能回答 `DeleteSnapshots` 或整轮 maintenance 的优化收益。

另有一份早期 512 行／8D 混合负载发生无完成事件的停滞，缺少失败现场栈，尚未定位；后续重复成功、回放成功与最新 trace 无停滞不等于已修复。已有受控栈采集与 watchdog 证据保留机制，后续继续保留。详见[受控 A/B](native-vortex-maintenance-probe-ab.md)与[开放停滞诊断](native-vortex-stall-diagnostics.md)。

当前没有与 Turbopuff 同数据／同硬件／同质量门槛的对照结果，也不能把这些小规模 fixture 的召回推广为任意数据集的质量保证。

## 10. 统一 roadmap

本表重新按 DuckLake 实际路线编号；“已验证”均受第 1、8、9 节边界限制。原 Iceberg RFC 的阶段目标不能逐项直接标记完成。

| 阶段 | 目标与当前状态 | 退出条件／剩余工作 |
| --- | --- | --- |
| R0 原生文件与静态索引 | **已实现并验证**：Vortex 文件、物理身份、静态 SPFresh、原生 ABI、命名索引 | 保持 provider 与依赖闭包可重建；发布／合入单独检查 |
| R1 实时基本链路 | **已实现并验证**：零 B、即时 T/F 查询、受管 upsert/delete、回执、ReadView、写入背压 | 固定 schema／单进程范围内已有端到端及恢复证据 |
| R2 多 B 与自动维护 | **已实现并验证**：独立新增 B、三段预算、选择性合并、自动 flush/build | 保持追加不重建未选中 B、同视图全局 top-k 和现有预算 |
| R3 物理回收与读者保护 | **已实现并验证**：重写、稀疏 B→F、退休、意图 GC、旧视图、事务完成唤醒 | 已通过受控取消／崩溃边界；持续吞吐问题转入 R4 |
| R4 长期维护稳态 | **因本机负载波动暂缓，未完成**：[文件名预筛选](native-vortex-filename-prefilter.md)通过每侧 53,322 项路径 oracle 和 2,144 项真实元数据检查；预先提交协议后的受控 A/B 完成 768 次主采样，两个目标改善 5.28%／15.70%，但一个对照增加 3.84%，按规则撤回候选。生产引用检查未改，CPU／线程／事务／预算保持 | 稳定资源环境具备后再恢复 A/A、性能 A/B 与长期稳态验证；既有 A/A 未达 2% 门槛、候选撤回、积压有界与开放停滞未关闭的结论均保留 |
| R5 容量与产品契约 | **部分完成，本地确定性生命周期已覆盖**：[确定性矩阵](native-vortex-functional-matrix.md)、[显式请求 epoch](native-vortex-request-epochs.md)，[reference／WAL 契约](native-vortex-reference-wal-lifecycle.md)、[容量 admission 边界](native-vortex-capacity-admission.md)、[查询 scratch 生命周期](native-vortex-query-scratch-lifecycle.md)、[表 UUID 状态生命周期](native-vortex-table-uuid-lifecycle.md)与[退休历史／物理回收契约](native-vortex-retired-history.md)；已验证固定循环的原生块复用 | 更大数据、质量、延迟、吞吐、长期容量与总磁盘／RSS 资格仍待验证，依赖稳定负载的项目继续暂缓 |
| R6 云端与分布式 | **部分完成，整体未完成**：[协议模型](native-vortex-remote-publication.md)、[PostgreSQL／S3 backend](native-vortex-postgres-s3-backend.md)、[typed metadata／UUID／epoch 契约](native-vortex-postgres-typed-lifecycle.md)及 [UUID／发布／ReadView 绑定](native-vortex-bound-readviews.md)已做组件层实测：跨 UUID 隔离、原子退休引用、确切 typed 快照与旧私有副本、不确定提交和受预算恢复；对象仍为 opaque fixture，原生 SQL 仍限定 native DuckDB／本地 POSIX | Reader session 所有权／fencing 与 pin 回收、原生 codec／源和索引接线及真实 AWS／TLS／IAM、HA、分布式 fan-out／top-k 资格 |
| R7 SPFresh 原生在线更新 | **候选后续路线，未接入** | 先证明相对独立静态 B 的必要性；设计可持久化版本、并发快照、撤销／恢复与 GC 协议，再验证原生增删改接口 |

原 RFC 的静态索引、实时可见、B/F/T、compaction／GC 目标在 R0–R3 有对应实现；其 Iceberg 专属物化、云端 fencing 和分布式 serving 并未因 DuckLake 本地链路通过而完成。保留原 RFC 作为历史来源，本文件作为 DuckLake 路线的总纲。

“可增量接收业务更新”已经成立：新版本进入 T，再经 F 成为独立 B。它与“直接调用 SPFresh 原生在线更新”是两个不同交付项；当前 roadmap 不把后者作为本地基本链路的前置条件，也不承诺未经验证就替换现有方案。

## 11. 下一份验证性 Draft PR

**下一项推荐 R6 reader session 所有权／fencing 与持久 pin 的显式回收。** [统一绑定](native-vortex-bound-readviews.md)已连接 typed UUID、source／generation、manifest／发布、ReadView 与原子退休引用，真实 PG／MinIO 的旧视图和晚到请求验证通过。当前 private copy 在 reader 内存中，尚无 reader 崩溃后的持久 session 回收协议；Vane SQL／维护也尚未调用这些实验性组件，原生 C++／Rust、provider 与 native 限制保持。

1. 明确 reader session 的持久身份、fence 和私有副本所有权，以确定性握手验证活跃 session 不被误回收、崩溃 session 可显式回收、旧 session 在失去资格后不能继续访问对象；继续保留普通 SQL 事务保护，不以无证据的 TTL 撤销 pin。
2. 再完成 broker 原生调用、实际 source／index 编码与查询接线，保留普通事务的自动登记／保护、认证私有缓存和现有预算；现有 C++ LIST 文本 codec 与新 REAL[] 组件仍有显式接线边界，前置条件未齐备时不解除 native 限制。
3. R6 的真实 AWS／TLS／IAM 部署、分布式 fan-out、读位置等待、全局 top-k 与 HA 仍需真实端到端证据；STOP 与协议 metadata 累积也尚无总空间上界。
4. R5 的更大数据／质量与容量资格仍须单独声明输入、资源和退出条件；当前 fixture 及三轮块复用不替代持续容量验证。
5. 具备稳定资源环境后恢复 R4：保留既有 A/A 失败、候选撤回和未关闭稳态问题，按原门槛验证。R4 A/A、性能 A/B 与长期负载继续暂缓。

本地 R5 已覆盖确定性生命周期；[退休历史／物理回收契约](native-vortex-retired-history.md)继续区分逻辑释放、原生块复用与实际文件尺寸。R4 的开放问题保持未完成。R6 继续按真实后端、远程视图、分布式查询分别验收；R7 按收益证据决定。本轮不改变本地所有权范围、质量门槛或资源预算。

## 12. 文档与实现导航

| 入口 | 用途 |
| --- | --- |
| [基本链路](native-vortex-basic-flow.md)／[安装态入口](vane-native-vortex-proof.md) | 从空表到 T/F/B 的命名 SQL 查询 |
| [受管写入](native-vortex-keyed-writes.md)／[自动全链路](native-vortex-automatic-flow.md) | 业务 mutation、配置、幂等与自动维护 |
| [多 B](native-vortex-segments-proof.md)／[选择性合并](native-vortex-segment-merge-proof.md) | 静态索引的增量组织与段预算 |
| [数据重写](native-vortex-index-rewrite-proof.md)／[稀疏重写](native-vortex-sparse-rewrite-proof.md) | 回收被替换／删除版本的物理容量 |
| [源保留](native-vortex-source-retention.md)／[事务唤醒](native-vortex-transaction-gc-wakeup.md) | 文件删除、读者保护与最新持续验证 |
| [快照与计时](native-vortex-readview-maintenance-profile.md)／[A/B 决策](native-vortex-maintenance-probe-ab.md) | 实际查询版本、阶段观测与已撤回实验 |
| [实际 source GC 成本](native-vortex-source-gc-cost.md) | 从相同输入恢复，区分快照与文件历史规模，定位下一项查询优化 |
| [过期窗口候选 A/B](native-vortex-expiry-interval.md) | 两种方案的等价性、受控比较、撤回决定和可复现测试引用 |
| [有序快照查找 A/B](native-vortex-expiry-lookup.md) | 区间 oracle、元数据／内存开销、对照门槛失败与 A/A 后续验证 |
| [相同二进制 A/A 精度](native-vortex-source-gc-aa.md) | 冻结协议、重点／完整矩阵、区间与分段检查、进程观测和后续隔离方案 |
| [固定 CPU 集合 A/A](native-vortex-source-gc-affinity.md) | affinity 继承／恢复、线程设置保留、重点阶段结果与未启动完整矩阵的门槛 |
| [独立元数据重复诊断](native-vortex-source-gc-metadata.md) | 128 次独立 trace、外层 SQL 守恒分类、显式规划拆分及 SQL 范围外开销的后续验证 |
| [引用检查内部诊断](native-vortex-reference-diagnostics.md) | 私有真实调用计时、Fetch／路径循环拆分、默认构建资格校验与后续否定预筛选验证 |
| [文件名预筛选 oracle 与 A/B](native-vortex-filename-prefilter.md) | 独立路径／真实元数据等价性、匹配链接的 12 输入 A/B、对照门槛失败与撤回决定 |
| [确定性维度与查询契约](native-vortex-functional-matrix.md) | 不依赖稳定负载的 R5 功能验收、B/F/T、过滤与拒绝边界 |
| [容量 admission 边界](native-vortex-capacity-admission.md) | 有效行／向量、请求、T/F、保留历史、存储／lease、manifest／段数及事务恢复的固定输入验收 |
| [显式请求 epoch 与回执生命周期](native-vortex-request-epochs.md) | 单表原子清理、旧请求拒绝、推进重试、并发／取消／崩溃恢复 |
| [Reference／WAL 生命周期契约](native-vortex-reference-wal-lifecycle.md) | 调用方 JSON、共享命名引用、私有视图、原生 checkpoint 与故障恢复 |
| [查询 scratch 所有权与回收](native-vortex-query-scratch-lifecycle.md) | 登记、活跃锁、显式预算、删除意图及 SIGKILL／重开验收 |
| [表 UUID 状态生命周期](native-vortex-table-uuid-lifecycle.md) | 请求 UUID fence、原子 DROP／状态释放、后台任务与恢复契约 |
| [已退休表原生历史与物理回收](native-vortex-retired-history.md) | catalog 历史范围、实际 inline／表级配置、受预算回收、块复用与恢复 |
| [R6 远程对象所有权与原子发布](native-vortex-remote-publication.md) | 能力配置、owner fencing、同事务发布／回执、读者 pin、晚到上传屏障与独立有界模型；#91 历史基线 |
| [R6 PostgreSQL／S3 最小 backend](native-vortex-postgres-s3-backend.md) | 真实事务与对象版本、签名票据、独立 physical oracle、未知响应／竞争／SIGKILL 与受预算恢复；原生入口待接入 |
| [R6 PostgreSQL typed metadata／UUID／epoch](native-vortex-postgres-typed-lifecycle.md) | PG OID／IEEE／NULL／BYTEA、字段与行身份、全 uint64 窗口、满回执、原子退休、提交故障和独立历史模型；#93 历史基线 |
| [R6 Typed UUID／发布／ReadView 统一绑定](native-vortex-bound-readviews.md) | 新 v2 布局、复合外键、每 UUID 发布 CAS、原子退休引用、确切快照／副本、晚到请求和真实服务恢复；尚无原生远程 ANN |
| [原生查询核心](../src/vortex_query/src/lib.rs)／[ReadView](../src/storage/ducklake_vortex_read_view.cpp)／[optimizer](../src/storage/ducklake_vortex_optimizer.cpp) | 对照实现与当前常量 |

后续能力或验证结论变化时，同时更新本文的基线、roadmap 状态、边界和证据入口。单次 proof 的历史结果继续保留；被撤回的实现和未关闭的失败不在总纲中写成已完成。
