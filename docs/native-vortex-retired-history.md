# Native Vortex 已退休表的原生历史与物理回收契约

本轮承接 [表 UUID 状态生命周期](native-vortex-table-uuid-lifecycle.md)，验证本地 native DuckDB 元数据下，逻辑退休之后的原生历史、inline 表、源文件、generation 与 WAL。总入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。生产变更是在原生过期事务中清理已无历史引用的 table ID 配置；接口、元数据布局、依赖、事务保护和预算沿用 #89 的 `e4cd030bb8fbe657179c045d75a2a5f65e1fe3b1`。

验证保持 CPU **6–11**、DuckDB **threads=2**、原 SPFresh／线程环境、事务保护和预算。输入、事务屏障及故障阶段固定；耗时只观察，R4 A/A、性能 A/B 与长期稳态继续暂缓。

## 退休、历史过期与物理 unlink

| 对象 | 退休提交后 | 后续回收条件与行为 |
| --- | --- | --- |
| UUID 回执、epoch、managed／预算／policy、命名引用 | 随 DROP 原子释放 | 同名新表使用新 UUID；旧请求不能转换身份 |
| 原生 `ducklake_table`／column／stats 等历史 | 保留结束快照及旧 table ID | 没有剩余快照落在该表的有效区间时，原生过期事务清除相关记录 |
| `ducklake_metadata` 的 table-scoped 配置 | 保留旧 table ID 的格式等选项，供历史查询使用 | 最后一个原生历史引用消失后，同一过期事务清理该 table ID 的配置；catalog／schema 及其他表的配置保留 |
| 原生 inline 数据表及文件删除表 | 仍是原生数据库里的实际 SQL 表 | 同一过期事务移除 registration 并 DROP 实际表；失败必须同时回滚历史记录、registration 和 DDL |
| 旧源文件及统计／删除元数据 | 保留历史引用，文件仍在 | 过期事务先移除不再需要的原生引用并写入 scheduled deletion；collector 之后认证身份、持久化删除意图、unlink 并完成计账 |
| 旧 generation | 最后一个命名引用解除后 retired | generation collector 按现有 manifest／所有权协议回收；它不替代源历史过期 |
| 同名新表、其他活跃表 | 使用各自 UUID／table ID／引用 | 回收旧 UUID 不得删除其业务行、实际 B、inline 表或文件 |
| caller JSON、未登记文件、查询 scratch | 保持各自所有权 | 当前 source／generation collector 不接管这些对象；scratch 使用 [独立协议](native-vortex-query-scratch-lifecycle.md) |

原生过期逻辑见 [`DeleteSnapshots`](../src/storage/ducklake_metadata_manager.cpp)，受管源回收见 [source retention](../src/storage/ducklake_vortex_source_retention.cpp)。删除 inline SQL 表和删除 DuckLake 源文件是两个不同动作；前者也不承诺原生数据库文件立即缩小。

修复前可复现：旧 `ducklake_table` 历史已经归零，`ducklake_metadata` 仍保留该 ID 的 `experimental_data_file_format=vortex`。修复只对原生过期逻辑已经确认无剩余历史的 `deleted_table_ids`，删除 `scope='table' AND scope_id IN (...)`；错误直接回滚。它不会在逻辑退休时提前删除选项，也不扫描接管更早形成的孤儿记录。事务内 inline DROP 后的取消、故障、冲突和 SIGKILL 同时覆盖这项删除的原子性。

## 保留策略与预算的确切作用域

source retention 是 **catalog 级**策略。`keep_snapshots` 和 `min_age_seconds` 共同决定可以过期的历史快照，最新快照始终保留。过期会清理该 catalog 中所有不再被剩余快照需要的对象，并不只作用于某个已退休 UUID。验收包含另一个已退休表，以及仍在使用真实 B 的相邻表；前者按现有策略释放原生历史，后者及同名新表保持可查询。

普通 SQL 事务继续保有旧 DuckLake／原生快照。source collector 在过期和文件 I/O 前检查事务 gate；存在这些事务时，返回 `blocked_by_transactions=true`，不清理其历史或文件。读者结束后，由调用方显式重试。已完全打开的私有 ReadView 已持有认证副本，可以跨退休、历史过期、规范 source／generation GC 和 metadata checkpoint 完成原查询。

测试每轮只给 **1 个对象／64 MiB**，没有扩大生产的最多 16 个对象／64 MiB 上限。source 的对象数计入本轮过期快照数与完成源文件数；generation 的对象数计入完成的 generation 数。一次快照过期可能删除多条原生记录和实际 inline 表；这些限额不等于 SQL 行数、原生页数或执行耗时上限。

**字节预算不足只暂缓相应文件删除。** source collector 仍可使用剩余对象预算过期元数据快照，因此不能把“小字节预算”解释为整轮元数据完全不变。已有 intent 的部分 unlink 由同一协议显式续作，完成计账只发生一次。

源文件的完成字节数与实际删除文件尺寸逐项相符。generation 的 `collected_artifact_bytes` 按持久登记的 artifact 字节计账；实际删除清单还包含协议 JSON 等文件。验收分别记录计账字节、实际文件个数与实际字节，不把它们混为总磁盘预算。

已退休表的显式操作入口如下；每次调用只完成给定预算内的一轮工作：

```sql
-- catalog 所有者先明确允许的历史保留范围。
SELECT * FROM ducklake_set_vortex_source_retention('lake',true,1,0);
SELECT * FROM ducklake_vortex_source_retention_status('lake');
SELECT * FROM ducklake_collect_vortex_sources('lake',1,67108864);
SELECT * FROM ducklake_collect_vortex_generations('lake',1,67108864);
CHECKPOINT __ducklake_metadata_lake;
```

此处 checkpoint 的对象是已核对路径的原生 metadata attachment。`CHECKPOINT lake` 会执行 DuckLake 的 flush／过期／维护序列，不能代替这个操作。保持现有非强制 checkpoint 和事务保护，不自动重试或升级为 force，也不手动删除 WAL。

## 原生块复用、文件尺寸与回放期限

原生 inline 表被 DROP 后，checkpoint 可把不再使用的块纳入原生分配器的空闲空间。固定三轮 T-only 验收从 `pragma_storage_info` 直接读取已持久化 `block_id`，要求后续新 UUID 的表实际使用先前已释放的块，并逐行核对新业务内容。证据同时保留已用／空闲块、原生文件尺寸与 WAL 尺寸。

块可以复用，但数据库文件仍可能增长；collector 删除的源／索引字节、原生空闲块和文件尺寸是三个独立度量。三轮固定输入不构成长时间磁盘或 RSS 上界，也不证明生产吞吐和延迟。

退休幂等回放依赖原生历史认证。表名尚未重用且历史仍在时，同 UUID 退休返回原结果；历史过期后，无法认证的 UUID 明确失败。同名新表存在时，旧 UUID 继续返回 `STALE_TABLE`。本轮不增加独立 tombstone、兼容层、孤儿接管或 fallback。

## 确定性验收与恢复

[安装态 coordinator](../test/vane/test_vane_vortex_retired_history.py) 驱动 [native fixture](../test/vane/native_vortex_history_fixture.hpp)，并在每个 native 场景结束或 SIGKILL 后，启动全新的已安装 Vane 进程核对和显式恢复。

```bash
env -u VORTEX_SPFRESH_NATIVE VANE_RUNNER=local-fast python -I \
  test/vane/test_vane_vortex_retired_history.py \
  --driver /path/to/ducklake_vortex_write_driver \
  --cpus 6,7,8,9,10,11 --output-dir /fresh/retired-history-proof
```

逐项核对范围包括：

- 全部 **28 张原生元数据表、13 张 Vortex 元数据表**，实际注册的 inline 数据表及已存在的 inline 文件删除表。
- 原生表 UUID／table ID、快照、独立 typed row 模型，以及所有 data／indexes 文件的 SHA-256 和尺寸。
- T-only 与 B＋F＋T 退休／同名重建；普通旧读者、保留快照的时间旅行查询、另一个退休表与相邻真实 B 的隔离。
- disabled／age／keep 策略、事务内显式过期的 rollback、小字节预算、源与 generation 的对象／字节计账、未知文件及 caller reference 保持。
- 真实原生 UPDATE 冲突、取消、注入错误、实际 SIGKILL、WAL 重放、持久删除意图续作与显式恢复的幂等性。

测试专用 `QueryEnd` 回调在原生 inline DROP 已执行、外层过期事务尚未提交时暂停或注入故障，不为生产路径增加接口。真实冲突由另一个连接提交 retention counter UPDATE，随后外层事务竞争该行；测试必须看到实际冲突错误，并比较包括独立已提交更新在内的完整回滚状态。

崩溃阶段覆盖过期前、事务内 inline DROP 后、过期提交后、source intent／unlink／完成，以及 generation intent／部分 unlink／文件全部删除／完成和 checkpoint 前后。重开时先精确匹配已提交状态与当前文件，再显式续作；attach 本身不能被计为 maintenance 已恢复。普通 collector cutpoint 的取消使用现有 context interruption 检查；事务内 QueryEnd 的中断由测试回调显式注入。

公开场景各自使用一个独立进程，避免同一进程加载多个缓存路径下的静态 OpenMP runtime。签名检查、线程环境和预算不变，没有启用 duplicate-runtime 绕过变量。

本轮继续限定单进程本地 POSIX 所有权与已知 catalog；不把进程 SIGKILL 验收扩展为掉电、远程持久性或多进程 fencing 保证。

## 本轮验证结果与构件身份

专项验收通过 **4 组独立公开契约、33 个 native／全新安装态恢复场景，含 15 次实际 SIGKILL**。取消与注入错误分别覆盖七个阶段；原生 UPDATE 冲突必须匹配实际冲突错误，事务内 inline DROP 后的故障必须恢复全部原生记录和表级配置。

B＋F＋T 公开场景实际删除 11 个文件、42,809 字节，其中两份源文件为 18,352 字节；generation 按登记计账 22,651 字节，实际其目录删除 24,457 字节。相邻真实 B、同名新表的真实 B、caller JSON 与未知文件保持。T-only 目标表未生成或 unlink 自己的 Vortex 源文件；其实际 inline 表和无引用表级配置已被原生过期事务释放。

三轮各 512 行的 T-only 输入中，后两轮的新表实际复用已释放的 block ID **2、3、4**。checkpoint 后的 WAL 归零；写入后原生文件依次为 6,565,888、7,876,608、7,876,608 字节。另一 B＋F＋T 原生场景清理后有 33 个空闲块、26 个已用块，文件从 9,973,760 增至 15,478,784 字节。这些数字只对应固定 fixture，展示逻辑释放、块复用、WAL 与文件尺寸的区别。

修复前的匹配 provider 在新回归断言上失败，报 `Retired cycles accumulated orphaned native table configuration`。证据为 `/tmp/ducklake-vortex-history-old-provider-repro-20261106/report.json`；其冻结 bundle 为 `/tmp/ducklake-vortex-history-wheels-20261106`。没有把后续版本通过当作旧版本已经正确。

主报告为 `/tmp/ducklake-vortex-history-proof-v2-20261106/report.json`，SHA-256 **`b562a2c4d2aa75e008d2fda3a690d612e1a272a7d082eba32157e56c02f57ddc`**。报告核验执行前后相关源码、driver、`_native` 和已安装 provider 闭包，记录各子进程 CPU affinity 及未改动的线程环境，并确认恢复调用只显式续作已登记对象。

| 构件 | SHA-256 |
| --- | --- |
| _native | `8ca20bd4c8a4203f239ff6d785c45575241d3b2c8d1f8e9fc184d90de1d54876` |
| native write driver | `5474641220b95a323ad8d9f56d720a84f4ef162eed2939afa304d9ca586a9f9f` |
| DuckLake artifact | `70106b76f9951f9657d546c5b50eac7d8b5a0dde518c513bb7e6fbc6a40d2f3f` |
| Vortex artifact | `16cc10c2bb607f539618a0887a5551f41eae51c2bd981fdb063d3c2536a85495` |
| DuckLake 测试 wheel | `87593948483d062c5978e56588d549d4dae0b46669df66d776c9eb72d0d4bac8` |
| Vortex 测试 wheel | `d7cd646e0db78bbc5e2ac03c84030794165109150c8684e1445e59d388a30b8e` |

本轮环境为 `/tmp/ducklake-vane-history-v2-env-20261106`，wheel bundle 为 `/tmp/ducklake-vortex-history-wheels-v2-20261106`。使用增量 Release、非 editable 重装和公开 CI 测试签名；未进行发布。Vane、wrapper、vortex-index／SPFresh 的 source pin 保持，未增加依赖或兼容路径。

相关 basic／keyed／source／epoch／reference-WAL／UUID 六组回归全部通过；其中 UUID 再次通过 6 组公开契约、25 个 native／恢复场景和 5 次 SIGKILL。Vane base launcher 按独立进程通过 **1,222 个非 Ray、32 个 Ray 测试，15 项可选依赖跳过**。报告保留于 `/tmp/ducklake-vortex-history-regressions-20261106`。格式、copyleft、工作流 YAML、文档链接及执行前后源码／构件身份检查通过。
